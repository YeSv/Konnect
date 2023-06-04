using Konnect.Common;
using Konnect.Common.Extensions;
using Konnect.Mongo.Contracts;
using Konnect.Pipelines.Dataflow;
using Konnect.Pipelines.Dataflow.Channels;
using Simple.Dotnet.Utilities.Buffers;

using static Konnect.Pipelines.KM.Steps;

namespace Konnect.Pipelines.KM;

public sealed class PipelineConfiguration
{
    public int Parallelism { get; set; } = 4;
    public int BatchSize { get; set; } = 10_000;
    public int ProcessingBlockSize { get; set; } = 2_000;
    public TimeSpan StopTimeout { get; set; } = TimeSpan.FromSeconds(10);
}

public readonly record struct ExecutionDependencies(Mongo.Sink Sink, Kafka.Source Source, Kafka.Processor PreProcessor, Mongo.Processor PostProcessor, Kafka.Partitioner Partitioner);

public sealed record PipelineDependencies(ExecutionDependencies Execution, PipelineConfiguration Configuration, ObservabilityContext Observability);

public static class Pipeline
{
    public static async Task Run(PipelineDependencies deps, CancellationToken token)
    {
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(token);
        using var offsets = new Kafka.Offsets();
        using var source = deps.Execution.Source.WithObserver(Kafka.SourceObserver.ForContext(deps.Observability).Tap(o =>
        {
            var onRevoked = o.OnRevoked;
            o.OnRevoked = (c, tpo) =>
            {
                onRevoked?.Invoke(c, tpo);
                tpo.ForEach(t => offsets.RemovePartition(t.Partition));
            };
        }));

        offsets.WithObserver(new()
        {
            OnOffsetFlushed = t =>
            {
                source.StoreOffset(t);
                deps.Observability.OffsetFlushed();
            },
            OnProcessingError = e => e
                .Tap(lifetime, (e, l) => l.Cancel())
                .Tap(deps.Observability, (e, o) => o.UnrecoverableError(e, nameof(Steps.StoreOffset)))
        });

        var ctx = new StepContext(deps.Observability, lifetime);
        var (partitioner, partitions) = (deps.Execution.Partitioner, deps.Configuration.Parallelism);
        var (processors, complete) = Assemble(offsets, deps, lifetime);

        try 
        {
            while (true)
            {
                using var batch = source.Read(lifetime.Token);

                deps.Observability.LogEvents<Kafka.Event>(batch.Written);

                for (var i = 0; i < batch.Written; i++)
                {
                    var @event = batch.WrittenSpan[i];

                    var partition = GetPartition(@event, partitioner, deps.Configuration.Parallelism, ctx);

                    await processors[partition](@event);

                    deps.Observability.RecordProcessingLag(@event.Result.Message.Timestamp.UtcDateTime).RecordPartitionDistribution(partition);
                }
            }
        }
        finally
        {
            await complete.Tap(lifetime, (c, l) => l.Cancel())().WaitForPipelineCompletion(deps.Observability, (obs, ex) => obs.CompletionFailed(ex));
        }
    }

    // Process -> Batch -> Persist -> StoreOffset
    static (Func<Kafka.Event, ValueTask>[], Func<Task>) Assemble(Kafka.Offsets offsets, PipelineDependencies deps, CancellationTokenSource lifetime)
    {
        var ((sink, source, preProcessor, postProcessor, partitioner), configuration, observability) = deps;

        var stepContext = new StepContext(deps.Observability, lifetime);

        var completions = new List<Task>();
        var partitions = new Func<Kafka.Event, ValueTask>[configuration.Parallelism];
        
        // Scatter
        for (var i = 0; i < configuration.Parallelism; i++)
        {
            var partition = i;

            var (writer, readerTask) = BatchingChannel.Create<Kafka.Event>(
                configuration.ProcessingBlockSize,
                configuration.BatchSize,
                async (batch, token) =>
                {
                    using var eventsToFlush = new Rent<IMongoEvent>(batch.Length);
                    using var eventsToStore = new Rent<Kafka.Event>(batch.Length);

                    for (var i = 0; i < batch.Length; i++)
                    {
                        var @event = batch.Span[i];

                        if (@event.Lifetime.IsCancellationRequested) continue;

                        MarkInProgress(@event, offsets, stepContext);

                        var (filteredOut, mongoEvent) = await Process(@event, preProcessor, postProcessor, stepContext);

                        if (filteredOut) FilteredOut(@event, stepContext);
                        else if (!@event.Lifetime.IsCancellationRequested) eventsToFlush.Append(mongoEvent!);

                        eventsToStore.Append(@event);
                    }

                    if (eventsToFlush.HasSome) await Persist(eventsToFlush.WrittenMemory, sink, stepContext);
                    if (eventsToStore.HasSome) StoreOffset(eventsToStore.WrittenMemory, offsets, stepContext);
                },
                e => e.Tap(lifetime, (e, l) => l.Cancel()).Tap(observability, (e, o) => o.UnrecoverableError(e, nameof(BatchingChannel))), lifetime.Token);


            completions.Add(readerTask);
            partitions[i] = e => writer.WriteAsync(e, lifetime.Token);
        }

        return 
        (
            partitions, 
            () => Task.WhenAll(completions).CompletePipelineWithTimeout(deps.Configuration.StopTimeout, deps.Observability, o => o.TimeoutOnStopOccurred())
        );
    }
}