using Konnect.Common;
using Konnect.Kafka;
using Confluent.Kafka;
using Simple.Dotnet.Utilities.Buffers;
using Partitioner = Konnect.Kafka.Partitioner;

namespace Konnect.Pipelines.KK;

public readonly record struct StepContext(ObservabilityContext Observability, CancellationTokenSource Lifetime);

public static class Steps 
{
    public static Event MarkInProgress(Event @event, Offsets offsets, StepContext ctx)
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

    public static int GetPartition(Event @event, Partitioner partitioner, int partitions, StepContext ctx)
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

    public static async Task<(bool FilteredOut, Message<byte[], byte[]>? Message)> Process(Event @event, Processor processor, StepContext ctx)
    {
        try
        {
            using (ctx.Observability.MeasureStepTime(nameof(Process)))
            {
                var (type, message) = await processor(@event.Result.Message, ctx.Lifetime.Token);
                return (type is ResultType.FilteredOut, message);
            }
        }
        catch (Exception ex)
        {
            ctx.Lifetime.Cancel();
            ctx.Observability.ProcessingError(ex, @event, nameof(Process));
            throw;
        }
    }

    public static void FilteredOut(Event @event, StepContext ctx)
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

    public static async Task<int> Produce(ReadOnlyMemory<Message<byte[], byte[]>> messages, Sink sink, StepContext ctx)
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

    public static void StoreOffset(ReadOnlyMemory<Event> events, Offsets offsets, StepContext ctx)
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