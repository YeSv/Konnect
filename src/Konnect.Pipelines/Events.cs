using Konnect.Common.Extensions;
using Konnect.Kafka;
using Konnect.Mongo.Contracts;
using Konnect.Mongo.Contracts.Serde;
using Confluent.Kafka;

namespace Konnect.Pipelines;

public static class EventsExtensions
{
    public static Message<byte[], byte[]>? ToKafkaMessage(this IMongoEvent? @event, MongoEventSerializer serdeType = MongoEventSerializer.MessagePack)
    {
        if (@event == null) return null;

        var headers = new Headers();
        headers.Add(nameof(MongoEventSerializer), Serializers.Int32.Serialize((int)serdeType, default));
        headers.Add(nameof(MongoEventType), Serializers.Int32.Serialize((int)@event.Type, default));

        return new()
        {
            Headers = headers,
            Key = @event.SerializeKey(),
            Value = @event.Serialize(serdeType)
        };
    }

    public static IMongoEvent? ToEvent(this Message<byte[], byte[]>? message)
    {
        if (message == null) return null;

        var serde = message.ReadHeader(nameof(MongoEventSerializer)).Map(h => (MongoEventSerializer)Deserializers.Int32.Deserialize(h, h is null, default));
        var @eventType = message.ReadHeader(nameof(MongoEventType)).Map(h => (MongoEventType)Deserializers.Int32.Deserialize(h, h is null, default));

        return message.Value.Deserialize(@eventType, serde);
    }
}