using System.Text;
using App.Metrics;
using App.Metrics.Counter;
using App.Metrics.Gauge;
using Konnect.Common;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Konnect.Kafka;

public static class Metrics
{
    static readonly string Context = $"{RootMetrics.Context}.Kafka";

    public static CounterOptions ErrorsCount = new()
    {
        Context = Context,
        ResetOnReporting = true,
        MeasurementUnit = Unit.Errors,
        Name = nameof(ErrorsCount)
    };

    public static readonly GaugeOptions ConsumerLag = new()
    {
        Context = Context,
        Name = nameof(ConsumerLag),
        MeasurementUnit = Unit.Items,
    };

    public static readonly GaugeOptions StoredOffset = new()
    {
        Context = Context,
        Name = nameof(StoredOffset),
        MeasurementUnit = Unit.Items,
    };

     public static class Tags
    {
        public static readonly string Topic = nameof(Topic);
        public static readonly string Partition = nameof(Partition);

        public static readonly string[] TopicPartitionContextGroup = { Topic, Partition, RootMetrics.Tags.ContextName };
    }
}

public static class Observability
{
    static readonly List<TopicPartition> EmptyAssignment = new();

    public static ObservabilityContext KafkaError(this ObservabilityContext ctx, Error error)
    {
        ctx.Metrics.Measure.Counter.Increment(Metrics.ErrorsCount, new(RootMetrics.Tags.ContextName, ctx.GetName()), 1);
        ctx.Logger.CreateLogger<Common.Token.Konnect>().LogWarning($"""Kafka error occurred for context "{ctx.GetName()}" {error}""");
        return ctx;
    }

    public static ObservabilityContext KafkaMessage(this ObservabilityContext ctx, LogMessage message)
    {
        ctx.Logger.CreateLogger<Common.Token.Konnect>().LogInformation($"""Kafka log for context "{ctx.GetName()}": [{message.Level}|{message.Facility}|{message.Name})]: "{message.Message}" """);
        return ctx;
    }

    public static ObservabilityContext KafkaStatistics(this ObservabilityContext ctx, Exception error)
    {
        ctx.Logger.CreateLogger<Source>().LogWarning($"""An error occurred parsing kafka statistics for context "{ctx.GetName()}" """);
        return ctx;
    }

    public static ObservabilityContext KafkaStatistics(this ObservabilityContext ctx, Statistics stats)
    {
        foreach (var topic in stats.Topics.Values)
        {
            foreach (var (partition, statistics) in topic.Partitions)
            {
                if (statistics.PartitionNum < 0) continue;
                
                var tags = new MetricTags(Metrics.Tags.TopicPartitionContextGroup, new[] { topic.TopicName, partition, ctx.GetName() });
                
                ctx.Metrics.Measure.Gauge.SetValue(Metrics.ConsumerLag, tags, Normalize(statistics.ConsumerLag, statistics.Desired));
                ctx.Metrics.Measure.Gauge.SetValue(Metrics.StoredOffset, tags, Normalize(statistics.StoredOffset, statistics.Desired));
            }
        }

        return ctx;

        static long Normalize(long value, bool desired) => value < 0 || !desired ? 0 : value;
    }

    public static ObservabilityContext Assigned(this ObservabilityContext ctx, IConsumer<byte[], byte[]> consumer, List<TopicPartition> assignment)
    {
        var (current, @new) = (consumer.Assignment ?? EmptyAssignment, assignment ?? EmptyAssignment);
        ctx.Logger.CreateLogger<Common.Token.Konnect>().LogInformation($"""Partition assignment occurred for "{ctx.GetName()}". Current: {FormatAssignment(current)}. New: {FormatAssignment(@new)} """);

        return ctx;

        static string FormatAssignment(List<TopicPartition> partitions)
        {
            if (partitions.Count <= 10) return string.Join(",", partitions.Select(tp => tp.ToString()));

            var builder = new StringBuilder();
            foreach (var topicPartition in partitions) builder.AppendLine(topicPartition.ToString());
            return builder.ToString();
        }
    }

    public static ObservabilityContext Revoked(this ObservabilityContext ctx, IConsumer<byte[], byte[]> consumer, List<TopicPartitionOffset> revoked)
    {
        ctx.Logger.CreateLogger<Source>().LogInformation($"""Partition revoke occurred for "{ctx.GetName()}". Revoked: {FormatRevoke(revoked)} """);
        return ctx;

        static string FormatRevoke(List<TopicPartitionOffset> partitions)
        {
            if (partitions.Count <= 10) return string.Join(",", partitions.Select(tp => tp.ToString()));

            var builder = new StringBuilder();
            foreach (var topicPartition in partitions) builder.AppendLine(topicPartition.ToString());
            return builder.ToString();
        }
    }
}