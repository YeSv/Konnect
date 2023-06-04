using Konnect.Pipelines.Dataflow;
using Konnect.Mongo;
using Konnect.Common.Extensions;
using Konnect.Common;
using Konnect.Pipelines.Dataflow.Channels;
using Simple.Dotnet.Utilities.Buffers;

using static Konnect.Pipelines.MM.Steps;
using Konnect.Mongo.Contracts;

namespace Konnect.Pipelines.MM;

public sealed class PipelineConfiguration
{
    public int Parallelism { get; set; } = 4;
    public int BatchSize { get; set; } = 10_000;
    public int ProcessingBlockSize { get; set; } = 2_000;
    public bool IgnoreStoredOffset { get; set; } = false;
    public TimeSpan StopTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan ReadPause { get; set; } = TimeSpan.FromMilliseconds(300);
}

public readonly record struct ExecutionDependencies(Sink Sink, Source Source, Offsets Offsets, Processor Processor, Partitioner Partitioner);

public sealed record PipelineDependencies(ExecutionDependencies Execution, PipelineConfiguration Configuration, ObservabilityContext Observability);

public static class Pipeline
{
    public static async Task Run(PipelineDependencies deps, CancellationToken token)
    {
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(token);

        using var source = deps.Execution.Source;
        using var offsets = deps.Execution.Offsets.WithObserver(new()
        {
            OnFlush = () => deps.Observability.OffsetFlushed(),
            OnFlushError = e => e
                .Tap(lifetime, (e, l) => l.Cancel())
                .Tap(deps.Observability, (e, obs) => obs.FlushFailed(e))
        });

        var ctx = new StepContext(deps.Observability, lifetime);
        var (partitioner, partitions) = (deps.Execution.Partitioner, deps.Configuration.Parallelism);
        var (processors, complete) = Assemble(deps, lifetime);

        try
        {
            var existingOffset = (deps.Configuration.IgnoreStoredOffset switch
            {
                true => default,
                false => await offsets.GetCommittedOffset(lifetime.Token)
            }).Tap(deps.Observability, (o, obs) => obs.LogOffset(o));

            if (existingOffset is not null) source.StartFrom(existingOffset!.Value);

            while (true)
            {
                using var batch = await source.Read(lifetime.Token);
                if (!batch.HasSome)
                {
                    await Task.Delay(deps.Configuration.ReadPause, lifetime.Token);
                    continue;
                }

                deps.Observability.LogEvents<Event>(batch.Written);

                for (var i = 0; i < batch.Written; i++)
                {
                    var @event = batch.WrittenSpan[i];

                    var partition = GetPartition(@event, partitioner, deps.Configuration.Parallelism, ctx);

                    await processors[partition](@event);

                    deps.Observability.RecordProcessingLag(@event.MongoEvent.ClusterTime).RecordPartitionDistribution(partition);
                }
            }
        }
        finally
        {
            await complete.Tap(lifetime, (c, l) => l.Cancel())().WaitForPipelineCompletion(deps.Observability, (obs, ex) => obs.CompletionFailed(ex));
        }
    }

    // Process -> Batch -> Persist -> StoreOffset
    static (Func<Event, ValueTask>[], Func<Task>) Assemble(PipelineDependencies deps, CancellationTokenSource lifetime)
    {
        var ((sink, source, offsets, processor, partitioner), configuration, observability) = deps;
        var (completions, partitions) = (new List<Task>(), new Func<Event, ValueTask>[configuration.Parallelism]);

        var stepContext = new StepContext(deps.Observability, lifetime);

        // Scatter
        for (var i = 0; i < configuration.Parallelism; i++)
        {
            var partition = i;

            var (writer, readerTask) = BatchingChannel.Create<Event>(
                configuration.ProcessingBlockSize,
                configuration.BatchSize,
                async (batch, token) =>
                {
                    using var eventsToStore = new Rent<Event>(batch.Length);
                    using var eventsToFlush = new Rent<IMongoEvent>(batch.Length);

                    for (var i = 0; i < batch.Length; i++)
                    {
                        var @event = batch.Span[i];

                        MarkInProgress(@event, offsets, stepContext);

                        var (filteredOut, mongoEvent) = await Process(@event, processor, stepContext);

                        if (filteredOut) FilteredOut(@event, stepContext);
                        else eventsToFlush.Append(mongoEvent!);

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