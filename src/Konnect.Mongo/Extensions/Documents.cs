using MongoDB.Bson;
using MongoDB.Bson.Serialization;

namespace Konnect.Mongo.Extensions;

public static class DocumentsExtensions
{
    public static RawBsonDocument AsRawBson<T>(this T value) => new(value.ToBson());

    public static BsonDocument ExtractBsonDocument(this RawBsonDocument doc) =>
        BsonSerializer.Deserialize<BsonDocument>(doc.Slice.AccessBackingBytes(0).Array)!;

    public static T ExtractValue<T>(this RawBsonDocument doc) =>
        BsonSerializer.Deserialize<T>(doc.Slice.AccessBackingBytes(0).Array);

    public static T ExtractValue<T>(this BsonDocument doc) => BsonSerializer.Deserialize<T>(doc);
}