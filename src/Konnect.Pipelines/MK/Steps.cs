using Konnect.Common;
using Konnect.Common.Extensions;
using Konnect.Mongo.Contracts;
using Konnect.Pipelines.Dataflow;
using Confluent.Kafka;
using Simple.Dotnet.Utilities.Buffers;

namespace Konnect.Pipelines.MK;

public readonly record struct StepContext(ObservabilityContext Observability, CancellationTokenSource Lifetime);

public static class Steps
{
    public static void MarkInProgress(Mongo.Event @event, Mongo.Offsets offsets, StepContext ctx)
    {
        try
        {
            offsets.InProgress(@event);
        }
        catch (Exception ex)
        {
            ctx.Lifetime.Cancel();
            ctx.Observability.UnrecoverableError(ex, nameof(MarkInProgress));
            throw;
        }
    }

    public static int GetPartition(Mongo.Event @event, Mongo.Partitioner partitioner, int partitions, StepContext ctx)
    {
        try
        {            
            return partitioner(@event.MongoEvent, partitions);
        }
        catch (Exception ex)
        {
            ctx.Observability.SkippableError(@event, ex, nameof(GetPartition));
            return 0;
        }
    }

    public static async Task<(bool FilteredOut, Message<byte[], byte[]>? Message)> Process(Mongo.Event @event, Mongo.Processor preProcessor, Kafka.Processor postProcessor, StepContext ctx)
    {
        try
        {
            using (ctx.Observability.MeasureStepTime(nameof(Process)))
            {
                var (preType, mongoEvent) = await preProcessor(@event.MongoEvent, ctx.Lifetime.Token);
                if (preType is Mongo.ResultType.FilteredOut) return (true, default);

                var (postType, message) = await postProcessor(mongoEvent.ToKafkaMessage()!, ctx.Lifetime.Token);  
                return (postType is Kafka.ResultType.FilteredOut, message);
            }
        }
        catch (Exception ex)
        {
            ctx.Lifetime.Cancel();
            ctx.Observability.ProcessingError(ex, @event, nameof(Process));
            throw;
        }
    }

    public static void FilteredOut(Mongo.Event @event, StepContext ctx)
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

    public static async Task<int> Produce(ReadOnlyMemory<Message<byte[], byte[]>> messages, Kafka.Sink sink, MongoEventSerializer serializer, StepContext ctx)
    {
        try
        {
            if (messages.Length == 0) return 0;

            using (ctx.Observability.MeasureStepTime(nameof(Produce)))
            {
                await sink.Write(messages, ctx.Lifetime.Token);
                ctx.Observability.RecordSinkMetrics(messages.Length);

                return messages.Length;
            }
        }
        catch (Exception ex)
        {
            ctx.Lifetime.Cancel();
            ctx.Observability.UnrecoverableError(ex, nameof(Produce));
            throw;
        }
    }

    public static void StoreOffset(ReadOnlyMemory<Mongo.Event> events, Mongo.Offsets offsets, StepContext ctx)
    {
        try
        {
            for (var i = 0; i < events.Length; i++) offsets.StoreOffset(events.Span[i]);
        }
        catch (Exception ex)
        {
            ctx.Lifetime.Cancel();
            ctx.Observability.UnrecoverableError(ex, nameof(StoreOffset));
            throw;
        }
    }
}