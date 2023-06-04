using Konnect.Common;
using Konnect.Mongo.Contracts;
using Confluent.Kafka;
using Simple.Dotnet.Utilities.Buffers;

namespace Konnect.Pipelines.KM;

public readonly record struct StepContext(ObservabilityContext Observability, CancellationTokenSource Lifetime);

public static class Steps
{
    public static Kafka.Event MarkInProgress(Kafka.Event @event, Kafka.Offsets offsets, StepContext ctx)
    {
        try
        {
            using (ctx.Observability.MeasureStepTime(nameof(MarkInProgress)))
            {
                offsets.MarkInProgress(new(@event.Result.TopicPartitionOffset, @event.Lifetime));
                return @event;
            }
        }
        catch (Exception ex)
        {
            ctx.Lifetime.Cancel();
            ctx.Observability.UnrecoverableError(ex, nameof(MarkInProgress));
            throw;
        }
    }

    public static int GetPartition(Kafka.Event @event, Kafka.Partitioner partitioner, int partitions, StepContext ctx)
    {
        try
        {
            return partitioner(@event.Result.Message, partitions);
        }
        catch (Exception ex)
        {
            ctx.Observability.SkippableError(@event, ex, nameof(GetPartition));
            return 0;
        }
    }

    public static async Task<(bool FilteredOut, IMongoEvent? Event)> Process(Kafka.Event @event, Kafka.Processor preProcessor, Mongo.Processor postProcessor, StepContext ctx)
    {
        try
        {
            using (ctx.Observability.MeasureStepTime(nameof(Process)))
            {
                var (preType, message) = await preProcessor(@event.Result.Message, ctx.Lifetime.Token);
                if (preType is Kafka.ResultType.FilteredOut) return (true, default);

                var (postType, mongoEvent) = await postProcessor(message.ToEvent()!, ctx.Lifetime.Token);
                if (postType is Mongo.ResultType.FilteredOut) return (true, default);

                return (false, mongoEvent);
            }
        }
        catch (Exception ex)
        {
            ctx.Lifetime.Cancel();
            ctx.Observability.ProcessingError(ex, @event, nameof(Process));
            throw;
        }
    }

    public static void FilteredOut(Kafka.Event @event, StepContext ctx)
    {
        try
        {
            ctx.Observability.FilteredOut(@event);
        }
        catch (Exception ex)
        {
            ctx.Observability.SkippableError(@event, ex, nameof(FilteredOut));
        }
    }

    public static async Task<int> Persist(ReadOnlyMemory<IMongoEvent> events, Mongo.Sink sink, StepContext ctx)
    {
        try
        {
            if (events.Length == 0) return 0;

            using (ctx.Observability.MeasureStepTime(nameof(Persist)))
            {
                await sink.Write(events, ctx.Lifetime.Token);
                
                ctx.Observability.RecordSinkMetrics(events.Length);

                return events.Length;
            }
        }
        catch (Exception ex)
        {
            ctx.Lifetime.Cancel();
            ctx.Observability.UnrecoverableError(ex, nameof(Persist));
            throw;
        }
    }

    public static void StoreOffset(ReadOnlyMemory<Kafka.Event> events, Kafka.Offsets offsets, StepContext ctx)
    {
        try
        {
            for (var i = 0; i < events.Length; i++)
            {
                var @event = events.Span[i];
                if (@event.Lifetime.IsCancellationRequested) continue;

                offsets.MarkToBeStored(new(@event.Result.TopicPartitionOffset, @event.Lifetime));
            }
        }
        catch (Exception ex)
        {
            ctx.Lifetime.Cancel();
            ctx.Observability.UnrecoverableError(ex, nameof(StoreOffset));
            throw;
        }
    }
}