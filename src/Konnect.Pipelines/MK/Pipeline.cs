using Konnect.Common;
using Konnect.Common.Extensions;
using Konnect.Mongo.Contracts;
using Konnect.Pipelines.Dataflow;
using Konnect.Pipelines.Dataflow.Channels;
using Confluent.Kafka;
using Simple.Dotnet.Utilities.Buffers;

using static Konnect.Pipelines.MK.Steps;

namespace Konnect.Pipelines.MK;

public sealed class PipelineConfiguration
{
    public int Parallelism { get; set; } = 4;
    public int BatchSize { get; set; } = 10_000;
    public int ProcessingBlockSize { get; set; } = 2_000;
    public bool IgnoreStoredOffset { get; set; } = false;
    public TimeSpan StopTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan ReadPause { get; set; } = TimeSpan.FromMilliseconds(300);
    public MongoEventSerializer Serializer { get; set; } = MongoEventSerializer.MessagePack;
}

public readonly record struct ExecutionDependencies(Kafka.Sink Sink, Mongo.Source Source, Mongo.Offsets Offsets, Mongo.Processor PreProcessor, Kafka.Processor PostProcessor, Mongo.Partitioner Partitioner);

public sealed record PipelineDependencies(ExecutionDependencies Execution, PipelineConfiguration Configuration, ObservabilityContext Observability);

public static class Pipeline
{
    public static async Task Run(PipelineDependencies deps, CancellationToken token)
    {
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(token);

        using var source = deps.Execution.Source;
        using var sink = deps.Execution.Sink.WithObserver(Kafka.SinkObserver.ForContext(deps.Observability));
        using var offsets = deps.Execution.Offsets.WithObserver(new()
        {
            OnFlush = () => deps.Observability.OffsetFlushed(),
            OnFlushError = e => e
                .Tap(lifetime, (e, l) => l.Cancel())
                .Tap(deps.Observability, (e, o) => o.FlushFailed(e))
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

                deps.Observability.LogEvents<Mongo.Event>(batch.Written);

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
    static (Func<Mongo.Event, ValueTask>[], Func<Task>) Assemble(PipelineDependencies deps, CancellationTokenSource lifetime)
    {
        var ((sink, source, offsets, preProcessor, postProcessor, partitioner), configuration, observability) = deps;

        var completions = new List<Task>();
        var stepContext = new StepContext(deps.Observability, lifetime);
        var partitions = new Func<Mongo.Event, ValueTask>[configuration.Parallelism];
        
        // Scatter
        for (var i = 0; i < configuration.Parallelism; i++)
        {
            var partition = i;

            var (writer, readerTask) = BatchingChannel.Create<Mongo.Event>(
                configuration.ProcessingBlockSize,
                configuration.BatchSize,
                async (batch, token) =>
                {
                    using var eventsToStore = new Rent<Mongo.Event>(batch.Length);
                    using var messagesToFlush = new Rent<Message<byte[], byte[]>>(batch.Length);

                    for (var i = 0; i < batch.Length; i++)
                    {
                        var @event = batch.Span[i];

                        MarkInProgress(@event, offsets, stepContext);

                        var (filteredOut, message) = await Process(@event, preProcessor, postProcessor, stepContext);

                        if (filteredOut) FilteredOut(@event, stepContext);
                        else messagesToFlush.Append(message!);

                        eventsToStore.Append(@event);
                    }

                    if (messagesToFlush.HasSome) await Produce(messagesToFlush.WrittenMemory, sink, configuration.Serializer, stepContext);
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