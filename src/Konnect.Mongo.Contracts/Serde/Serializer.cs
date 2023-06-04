using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using Konnect.Mongo.Contracts.Serde.Json;
using Konnect.Mongo.Contracts.Serde.MsgPack;

namespace Konnect.Mongo.Contracts.Serde;

public static class MongoEventsSerializer
{
    public static byte[] Serialize(this IMongoEvent @event, MongoEventSerializer serializer = MongoEventSerializer.MessagePack) =>
        serializer switch
        {
            MongoEventSerializer.Json => JsonSerde.Serialize(@event),
            MongoEventSerializer.MessagePack => MessagePackSerde.Serialize(@event),
            var s => throw new Exception($"Unknown serializer type: {s}")
        };

    public static IMongoEvent Deserialize(this byte[] slice, MongoEventType? type = null, MongoEventSerializer serializer = MongoEventSerializer.MessagePack) =>
        serializer switch
        {
            MongoEventSerializer.Json => JsonSerde.Deserialize(slice, type),
            MongoEventSerializer.MessagePack => MessagePackSerde.Deserialize(slice, type),
            var s => throw new Exception($"Unknown serializer type: {s}")
        };

    public static byte[] SerializeKey(this IMongoEvent @event) => @event.GetKey() switch
    {
        null => Array.Empty<byte>(),
        var k => k.ToBson()
    };

    public static BsonDocument? DeserializeKey(this byte[]? slice) => slice switch
    {
        null or { Length: 0 } => null,
        var k => BsonSerializer.Deserialize<BsonDocument>(k)
    };
}