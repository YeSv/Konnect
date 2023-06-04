using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;

namespace Konnect.Mongo.Contracts.Serde.Json;

public static class JsonSerde
{
    static readonly JsonSerializerOptions Opts = new();

    static JsonSerde()
    {
        Opts.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
        
        Opts.Converters.Add(new JsonBsonDocumentConverter());
        Opts.Converters.Add(new JsonRawBsonDocumentConverter());
    }

    public static byte[] Serialize(this IMongoEvent @event) => @event switch
    {
        DropEvent d => JsonSerializer.SerializeToUtf8Bytes(d, Opts),
        InvalidateEvent i => JsonSerializer.SerializeToUtf8Bytes(i, Opts),
        UnsupportedEvent u => JsonSerializer.SerializeToUtf8Bytes(u, Opts),

        InsertEvent i => JsonSerializer.SerializeToUtf8Bytes(i, Opts),
        UpdateEvent u => JsonSerializer.SerializeToUtf8Bytes(u, Opts),
        ReplaceEvent r => JsonSerializer.SerializeToUtf8Bytes(r, Opts),
        DeleteEvent d => JsonSerializer.SerializeToUtf8Bytes(d, Opts),

        var t => throw new Exception($"Unknown event type {t.Type}")
    };

    public static IMongoEvent Deserialize(this byte[] @slice, MongoEventType? type = null) =>
        (type ?? JsonSerializer.Deserialize<MongoEventHeader>(@slice, Opts).Type) switch
        {
            MongoEventType.Drop => DropEvent.Shared,
            MongoEventType.Invalidate => InvalidateEvent.Shared,
            MongoEventType.Unsupported => JsonSerializer.Deserialize<UnsupportedEvent>(slice, Opts)!,

            MongoEventType.Insert => JsonSerializer.Deserialize<InsertEvent>(slice, Opts)!,
            MongoEventType.Update => JsonSerializer.Deserialize<UpdateEvent>(slice, Opts)!,
            MongoEventType.Replace => JsonSerializer.Deserialize<ReplaceEvent>(slice, Opts)!,
            MongoEventType.Delete => JsonSerializer.Deserialize<DeleteEvent>(slice, Opts)!,
            var t => throw new Exception($"Unknown event type: {t}")
        };
}

public sealed class JsonRawBsonDocumentConverter : JsonConverter<RawBsonDocument>
{
    public override RawBsonDocument? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.GetString() switch
        {
            null => null,
            var str => BsonSerializer.Deserialize<RawBsonDocument>(str)
        };

    public override void Write(Utf8JsonWriter writer, RawBsonDocument value, JsonSerializerOptions options) => writer.WriteStringValue(value.ToJson());
}

public sealed class JsonBsonDocumentConverter : JsonConverter<BsonDocument>
{
    public override BsonDocument? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        reader.GetString() switch
        {
            null => null,
            var str => BsonSerializer.Deserialize<RawBsonDocument>(str)
        };

    public override void Write(Utf8JsonWriter writer, BsonDocument value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value.ToJson());
}
