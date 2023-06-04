using Confluent.Kafka;

namespace Konnect.Kafka;

public static class KafkaExtensions 
{
    public static byte[]? ReadHeader<TK, TV>(this Message<TK, TV> message, string name) => message switch
    {
        null => null,
        { Headers: null or { Count: 0 } } => null,
        { Headers: var hs } => hs.FirstOrDefault(h => h.Key == name)?.GetValueBytes()
    };
}