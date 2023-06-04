using App.Metrics;
using App.Metrics.Counter;
using App.Metrics.Gauge;
using App.Metrics.Timer;
using Konnect.Common;
using Konnect.Common.Extensions;
using Konnect.Mongo.Contracts;

using Microsoft.Extensions.Logging;

using MongoDB.Bson;

namespace Konnect.Pipelines;

public static class PipelinesMetrics
{
    public static readonly string Context = $"{RootMetrics.Context}.Pipelines";

    public static readonly CounterOptions FilteredOutCount = new()
    {
        ResetOnReporting = true,
        Context = Context,
        Name = nameof(FilteredOutCount)
    };

    public static readonly CounterOptions LifetimeSkippedCount = new()
    {
        ResetOnReporting = true,
        Context = Context,
        Name = nameof(LifetimeSkippedCount)
    };

    public static readonly CounterOptions StepErrorsCount = new()
    {
        ResetOnReporting = true,
        Context = Context,
        Name = nameof(StepErrorsCount)
    };

    public static readonly CounterOptions ReceivedEvents = new()
    {
        Context = Context,
        ResetOnReporting = true,
        Name = nameof(ReceivedEvents)
    };

    public static readonly CounterOptions SentEvents = new()
    {
        Context = Context,
        ResetOnReporting = true,
        Name = nameof(SentEvents)
    };

    public static readonly TimerOptions StepDuration = new()
    {
        Context = Context,
        Name = nameof(StepDuration)
    };

    public static readonly CounterOptions OffsetFlushesCount = new()
    {
        Context = Context,
        ResetOnReporting = true,
        Name = nameof(OffsetFlushesCount)
    };

    public static readonly CounterOptions StopTimeouts = new()
    {
        Context = Context,
        ResetOnReporting = true,
        Name = nameof(StopTimeouts)
    };

    public static readonly GaugeOptions SinkBatchSize = new()
    {
        Context = Context,
        ResetOnReporting = true,
        MeasurementUnit = Unit.Items,
        Name = nameof(SinkBatchSize)
    };

    public static readonly GaugeOptions ProcessingLag = new()
    {
        Context = Context,
        ResetOnReporting = true,
        Name = nameof(ProcessingLag),
        MeasurementUnit = Unit.Items
    };


    public static readonly CounterOptions PartitionDistribution = new()
    {
        Context = Context,
        ResetOnReporting = true,
        Name = nameof(PartitionDistribution)
    };

    public static class Tags
    {
        public static readonly string Step = nameof(Step);
        public static readonly string Partition = nameof(Partition);

        public static readonly string[] ContextAndStep = new[] { RootMetrics.Tags.ContextName, Step };
        public static readonly string[] ContextAndPartition = new[] { RootMetrics.Tags.ContextName, Partition };
    }
}

public static class PipelinesObservability 
{
    public static IDisposable MeasureStepTime(this ObservabilityContext ctx, string step) =>
        ctx.Metrics.Measure.Timer.Time(PipelinesMetrics.StepDuration, new MetricTags(PipelinesMetrics.Tags.ContextAndStep, new [] { ctx.GetName(), step }));

    public static void LifetimeSkipped<T>(this ObservabilityContext ctx, T @event) =>
        ctx.Metrics.Measure.Counter.Increment(PipelinesMetrics.LifetimeSkippedCount, new(RootMetrics.Tags.ContextName, ctx.GetName()), 1);

    public static void FilteredOut<T>(this ObservabilityContext ctx, T @event) =>
        ctx.Metrics.Measure.Counter.Increment(PipelinesMetrics.FilteredOutCount, new(RootMetrics.Tags.ContextName, ctx.GetName()), 1);

    public static void SkippableError(this ObservabilityContext ctx, Kafka.Event @event, Exception ex, string step)
    {
        ctx.Metrics.Measure.Counter.Increment(PipelinesMetrics.StepErrorsCount, new(PipelinesMetrics.Tags.ContextAndStep, new [] { ctx.GetName(), step }), 1);
        ctx.Logger.CreateLogger<Kafka.Event>().LogWarning(ex, $"""Failed to process event during "{step}" step of "{ctx.GetName()}". TPO: {@event.Result.TopicPartitionOffset}""");
    }

    public static void SkippableError(this ObservabilityContext ctx, Mongo.Event @event, Exception ex, string step)
    {
        var (metrics, logger, _) = ctx;
        
        metrics.Measure.Counter.Increment(PipelinesMetrics.StepErrorsCount, new(PipelinesMetrics.Tags.ContextAndStep, new [] { ctx.GetName(), step }), 1);
        logger.CreateLogger<Kafka.Event>().LogWarning(ex, $"""Failed to process event during "{step}" step of "{ctx.GetName()}". Key: {@event.MongoEvent.GetKey()?.ToJson()}. Offset: {@event.Offset.Timestamp.Value}:{@event.Offset.Token}""");
    }

    public static Exception UnrecoverableError(this ObservabilityContext ctx, Exception ex, string step)
    {
        ctx.Metrics.Measure.Counter.Increment(PipelinesMetrics.StepErrorsCount, new(PipelinesMetrics.Tags.ContextAndStep, new [] { ctx.GetName(), step }), 1);
        ctx.Logger.CreateLogger<Kafka.Event>().LogError(ex, $"""Unhandled error occurred during "{step}" step of "{ctx.GetName()}" """);
        return ex;
    }

    public static Exception ProcessingError(this ObservabilityContext ctx, Exception ex, Kafka.Event @event, string step)
    {
        ctx.Metrics.Measure.Counter.Increment(PipelinesMetrics.StepErrorsCount, new(PipelinesMetrics.Tags.ContextAndStep, new[] { ctx.GetName(), step }), 1);
        ctx.Logger.CreateLogger<Kafka.Event>().LogError(ex, $"""Unhandled error occurred during "{step}" step of "{ctx.GetName()}". TPO: {@event.Result.TopicPartitionOffset} """);
        return ex;
    }

    public static Exception ProcessingError(this ObservabilityContext ctx, Exception ex, Mongo.Event @event, string step)
    {
        ctx.Metrics.Measure.Counter.Increment(PipelinesMetrics.StepErrorsCount, new(PipelinesMetrics.Tags.ContextAndStep, new[] { ctx.GetName(), step }), 1);
        ctx.Logger.CreateLogger<Mongo.Event>().LogError(ex, $"""Unhandled error occurred during "{step}" step of "{ctx.GetName()}". Key: {@event.MongoEvent.GetKey()?.ToJson()}. Offset: {@event.Offset.Timestamp.Value}:{@event.Offset.Token}""");
        return ex;
    }

    public static void LogEvents<T>(this ObservabilityContext ctx, int total) =>
        ctx.Metrics.Measure.Counter.Increment(PipelinesMetrics.ReceivedEvents, new(RootMetrics.Tags.ContextName, ctx.GetName()), total);

    public static void LogOffset(this ObservabilityContext obs, Mongo.Offset? offset)
    {
        if (offset is null) obs.Logger.CreateLogger<Mongo.Offset>().LogInformation($"""No offset was stored for "{obs.GetName()}" """);
        else obs.Logger.CreateLogger<Mongo.Offset>().LogInformation($"""Found stored offset for "{obs.GetName()}". Doc: {offset.Value.Token.ToJson()}. Timestamp: {offset.Value.Timestamp.Value} """);
    }

    public static void OffsetFlushed(this ObservabilityContext ctx) =>
        ctx.Metrics.Measure.Counter.Increment(PipelinesMetrics.OffsetFlushesCount, new(RootMetrics.Tags.ContextName, ctx.GetName()), 1);

    public static void FlushFailed(this ObservabilityContext ctx, Exception ex) =>
        ctx.Logger.CreateLogger<Kafka.Event>().LogWarning(ex , $"""Failed to flush offset for "{ctx.GetName()}". Pipeline will be stopped""");

    public static void CompletionFailed(this ObservabilityContext ctx, Exception ex) =>
        ctx.Logger.CreateLogger<Common.Token.Konnect>().LogWarning(ex, $"""Failed to gracefully complete pipeline for "{ctx.GetName()}" """);

    public static void TimeoutOnStopOccurred(this ObservabilityContext ctx)
    {
        ctx.Metrics.Measure.Counter.Increment(PipelinesMetrics.StopTimeouts, new(RootMetrics.Tags.ContextName, ctx.GetName()), 1);
        ctx.Logger.CreateLogger<Common.Token.Konnect>().LogWarning($"""Timeout on stop occurred for "{ctx.GetName()}" """);
    }

    public static void RecordSinkMetrics(this ObservabilityContext ctx, int size)
    {
        ctx.Metrics.Measure.Counter.Increment(PipelinesMetrics.SentEvents, new MetricTags(RootMetrics.Tags.ContextName, ctx.GetName()), size);
        ctx.Metrics.Measure.Gauge.SetValue(PipelinesMetrics.SinkBatchSize, new MetricTags(RootMetrics.Tags.ContextName, ctx.GetName()), size);
    }

    public static ObservabilityContext RecordProcessingLag(this ObservabilityContext ctx, DateTime sourceTime) => ctx.Tap(sourceTime, (c, t) =>
        c.Metrics.Measure.Gauge.SetValue(PipelinesMetrics.ProcessingLag, new(RootMetrics.Tags.ContextName, ctx.GetName()), (long)(DateTime.UtcNow - sourceTime).TotalMilliseconds));


    public static ObservabilityContext RecordPartitionDistribution(this ObservabilityContext ctx, int partition) => ctx.Tap(partition, (c, t) =>
        c.Metrics.Measure.Counter.Increment(PipelinesMetrics.PartitionDistribution, new(PipelinesMetrics.Tags.ContextAndPartition, new[] { ctx.GetName(), partition.ToString() }), 1));
}