using MessagePack;
using MessagePack.Formatters;
using MessagePack.Resolvers;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using System.Buffers;

namespace Konnect.Mongo.Contracts.Serde.MsgPack;

public static class MessagePackSerde
{
    static readonly MessagePackSerializerOptions Opts = MessagePackSerializerOptions.Standard
        .WithResolver(
            CompositeResolver.Create(
                new IMessagePackFormatter[] { new MessagePackBsonDocumentFormatter(), new MessagePackRawBsonDocumentFormatter() }, 
                new IFormatterResolver[] { StandardResolver.Instance }))
        .WithCompression(MessagePackCompression.Lz4BlockArray);

    public static byte[] Serialize(this IMongoEvent @event) => @event switch
    {
        DropEvent d => MessagePackSerializer.Serialize(d, Opts),
        InvalidateEvent i => MessagePackSerializer.Serialize(i, Opts),
        UnsupportedEvent u => MessagePackSerializer.Serialize(u, Opts),

        InsertEvent i => MessagePackSerializer.Serialize(i, Opts),
        UpdateEvent u => MessagePackSerializer.Serialize(u, Opts),
        ReplaceEvent r => MessagePackSerializer.Serialize(r, Opts),
        DeleteEvent d => MessagePackSerializer.Serialize(d, Opts),

        var t => throw new Exception($"Unknown event type {t.Type}")
    };

    public static IMongoEvent Deserialize(this byte[] slice, MongoEventType? type = null) =>
        (type ?? MessagePackSerializer.Deserialize<MongoEventHeader>(slice, Opts).Type) switch
        {
            MongoEventType.Drop => DropEvent.Shared,
            MongoEventType.Invalidate => InvalidateEvent.Shared,
            MongoEventType.Unsupported => MessagePackSerializer.Deserialize<UnsupportedEvent>(slice, Opts),

            MongoEventType.Insert => MessagePackSerializer.Deserialize<InsertEvent>(slice, Opts),
            MongoEventType.Update => MessagePackSerializer.Deserialize<UpdateEvent>(slice, Opts),
            MongoEventType.Replace => MessagePackSerializer.Deserialize<ReplaceEvent>(slice, Opts),
            MongoEventType.Delete => MessagePackSerializer.Deserialize<DeleteEvent>(slice, Opts),
            var t => throw new Exception($"Unknown event type: {t}")
        };
}

public sealed class MessagePackBsonDocumentFormatter : IMessagePackFormatter<BsonDocument?>
{
    public BsonDocument? Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil()) return null;

        options.Security.DepthStep(ref reader);

        var bytes = reader.ReadBytes()!.Value.ToArray();

        reader.Depth--;
        return BsonSerializer.Deserialize<BsonDocument>(bytes);
    }

    public void Serialize(ref MessagePackWriter writer, BsonDocument? value, MessagePackSerializerOptions options)
    {
        if (value == null)
        {
            writer.WriteNil();
            return;
        }

        writer.Write(value.ToBson());
    }
}

public sealed class MessagePackRawBsonDocumentFormatter : IMessagePackFormatter<RawBsonDocument?>
{
    public RawBsonDocument? Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil()) return null;

        options.Security.DepthStep(ref reader);

        var bytes = reader.ReadBytes()!.Value.ToArray();

        reader.Depth--;
        return new(bytes);
    }

    public void Serialize(ref MessagePackWriter writer, RawBsonDocument? value, MessagePackSerializerOptions options)
    {
        if (value == null)
        {
            writer.WriteNil();
            return;
        }

        writer.Write(value.Slice.AccessBackingBytes(0));
    }
}
