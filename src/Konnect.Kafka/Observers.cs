using Konnect.Common;
using Confluent.Kafka;

namespace Konnect.Kafka;

public sealed class SourceObserver
{
    public Action<IConsumer<byte[], byte[]>, Error> OnError { get; set; } = (c, e) => {};
    public Action<IConsumer<byte[], byte[]>, LogMessage> OnLog { get; set; } = (c, m) => {};
    public Action<IConsumer<byte[], byte[]>, string> OnStatistics { get; set; } = (c, s) => {};
    public Action<IConsumer<byte[], byte[]>, List<TopicPartition>> OnAssigned { get; set; } = (c, t) => {};
    public Action<IConsumer<byte[], byte[]>, List<TopicPartitionOffset>> OnRevoked { get; set; } = (c, t) => {};

    public static SourceObserver ForContext(ObservabilityContext ctx) => new()
    {
        OnAssigned = (c, tp) => ctx.Assigned(c, tp),
        OnRevoked = (c, tpo) => ctx.Revoked(c, tpo),
        OnError = (c, e) => ctx.KafkaError(e),
        OnLog = (c, l) => ctx.KafkaMessage(l),
        OnStatistics = (c, s) => _ = Statistics.FromJson(s) switch
        {
            (var stats, null) => ctx.KafkaStatistics(stats!),
            (null, var error) => ctx.KafkaStatistics(error!),
            _ => ctx
        }
    };
}

public sealed class SinkObserver
{
    public Action<IProducer<byte[], byte[]>, Error> OnError { get; set; } = (p, e) => {};
    public Action<IProducer<byte[], byte[]>, LogMessage> OnLog { get; set; } = (p, m) => {};
    public Action<IProducer<byte[], byte[]>, string> OnStatistics { get; set; } = (p, s) => {};

    public static SinkObserver ForContext(ObservabilityContext ctx) => new()
    {
        OnError = (p, e) => ctx.KafkaError(e),
        OnLog = (p, l) => ctx.KafkaMessage(l) 
    };
}