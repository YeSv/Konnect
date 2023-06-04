using System.Threading.Tasks.Dataflow;
using Konnect.Common;
using Konnect.Common.Extensions;
using Konnect.Kafka;
using Konnect.Pipelines.Dataflow;
using Konnect.Pipelines.Dataflow.Channels;
using Confluent.Kafka;
using Simple.Dotnet.Utilities.Buffers;

using Partitioner = Konnect.Kafka.Partitioner;
using static Konnect.Pipelines.KK.Steps;

namespace Konnect.Pipelines.KK;

public sealed class PipelineConfiguration
{
    public int Parallelism { get; set; } = 4;
    public int BatchSize { get; set; } = 10_000;
    public int ProcessingBlockSize { get; set; } = 2000;
    public TimeSpan StopTimeout { get; set; } = TimeSpan.FromSeconds(10);
}

public readonly record struct ExecutionDependencies(Sink Sink, Source Source, Processor Processor, Partitioner Partitioner);

public sealed record PipelineDependencies(ExecutionDependencies Execution, PipelineConfiguration Configuration, ObservabilityContext Observability);

public static class Pipeline 
{
    public static async Task Run(PipelineDependencies deps, CancellationToken token)
    {
        using var offsets = new Offsets();
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(token);
        using var sink = deps.Execution.Sink.WithObserver(SinkObserver.ForContext(deps.Observability));
        using var source = deps.Execution.Source.WithObserver(SourceObserver.ForContext(deps.Observability).Tap(o =>
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

        var (partitioner, partitions) = (deps.Execution.Partitioner, deps.Configuration.Parallelism);

        var ctx = new StepContext(deps.Observability, lifetime);
        var (processors, complete) = Assemble(offsets, deps, lifetime);

        try 
        {
            while (true)
            {
                using var batch = source.Read(lifetime.Token);

                deps.Observability.LogEvents<Event>(batch.Written);
                
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

    // Process -> Batch -> Produce -> StoreOffset
    static (Func<Event, ValueTask>[], Func<Task>) Assemble(Offsets offsets, PipelineDependencies deps, CancellationTokenSource lifetime)
    {
        var ((sink, source, processor, partitioner), configuration, observability) = deps;

        var stepContext = new StepContext(deps.Observability, lifetime);

        var completions = new List<Task>();
        var partitions = new Func<Event, ValueTask>[configuration.Parallelism];
        
        for (var i = 0; i < configuration.Parallelism; i++)
        {
            var partition = i;

            var (writer, readerTask) = BatchingChannel.Create<Event>(
                configuration.ProcessingBlockSize,
                configuration.BatchSize,
                async (batch, token) =>
                {
                    using var eventsToStore = new Rent<Event>(batch.Length);
                    using var messagesToFlush = new Rent<Message<byte[], byte[]>>(batch.Length);

                    for (var i = 0; i < batch.Length; i++)
                    {
                        var @event = batch.Span[i];

                        if (@event.Lifetime.IsCancellationRequested) continue;

                        MarkInProgress(@event, offsets, stepContext);

                        var (filteredOut, message) = await Process(@event, processor, stepContext);

                        if (filteredOut) FilteredOut(@event, stepContext);
                        else if (!@event.Lifetime.IsCancellationRequested) messagesToFlush.Append(message!);

                        eventsToStore.Append(@event);
                    }

                    if (messagesToFlush.HasSome) await Produce(messagesToFlush.WrittenMemory, sink, stepContext);
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