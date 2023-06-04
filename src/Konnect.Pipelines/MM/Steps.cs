using Konnect.Common;
using Konnect.Mongo;
using Konnect.Mongo.Contracts;
using Simple.Dotnet.Utilities.Buffers;

namespace Konnect.Pipelines.MM;

public readonly record struct StepContext(ObservabilityContext Observability, CancellationTokenSource Lifetime);

public static class Steps
{
    public static void MarkInProgress(Event @event, Offsets offsets, StepContext ctx)
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

    public static int GetPartition(Event @event, Partitioner partitioner, int partitions, StepContext ctx)
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

    public static async Task<(bool FilteredOut, IMongoEvent? Event)> Process(Event @event, Processor processor, StepContext ctx)
    {
        try
        {
            using (ctx.Observability.MeasureStepTime(nameof(Process)))
            {
                var (resultType, mongoEvent) = await processor(@event.MongoEvent, ctx.Lifetime.Token);
                return (resultType is ResultType.FilteredOut, mongoEvent);
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

    public static async Task<int> Persist(ReadOnlyMemory<IMongoEvent> events, Sink sink, StepContext ctx)
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

    public static void StoreOffset(ReadOnlyMemory<Event> events, Offsets offsets, StepContext ctx)
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