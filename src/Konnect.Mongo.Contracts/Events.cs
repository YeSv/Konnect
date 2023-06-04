using MongoDB.Bson;
using System.Runtime.Serialization;

namespace Konnect.Mongo.Contracts;

public enum MongoEventType : byte { Invalidate, Insert, Delete, Replace, Update, Drop, Unsupported }

public enum MongoEventSerializer : byte { Json, MessagePack }

public interface IMongoEvent
{
    public MongoEventType Type { get; }
    public DateTime ClusterTime { get; }
}

[DataContract]
public struct MongoEventHeader
{
    public MongoEventHeader(MongoEventType type) => Type = type;
    [DataMember(Order = 0)] public MongoEventType Type { get; set; }
}

[DataContract]
public sealed class InvalidateEvent : IMongoEvent
{
    public static readonly InvalidateEvent Shared = new();
    
    [DataMember(Order = 0)] public MongoEventType Type => MongoEventType.Invalidate;

    [DataMember(Order = 1)] public DateTime ClusterTime { get; } = DateTime.UtcNow;
}

[DataContract]
public sealed class DropEvent : IMongoEvent
{
    public static readonly DropEvent Shared = new();

    [DataMember(Order = 0)] public MongoEventType Type => MongoEventType.Drop;

    [DataMember(Order = 1)] public DateTime ClusterTime { get; set; } = DateTime.UtcNow;
}

[DataContract]
public sealed class InsertEvent : IMongoEvent
{
    public InsertEvent() {}

    public InsertEvent(BsonDocument key, BsonDocument document, DateTime clusterTime) => (Key, Document, ClusterTime) = (key, document, clusterTime);

    [DataMember(Order = 0)] public MongoEventType Type => MongoEventType.Insert;

    [DataMember(Order = 1)] public BsonDocument Key { get; set; } = new();

    [DataMember(Order = 2)] public BsonDocument Document { get; set; } = default!;
    
    [DataMember(Order = 3)] public DateTime ClusterTime { get; set; } = DateTime.UtcNow;
}

[DataContract]
public sealed class DeleteEvent : IMongoEvent
{
    public DeleteEvent() {}

    public DeleteEvent(BsonDocument key, DateTime clusterTime) => (Key, ClusterTime) = (key, clusterTime);

    [DataMember(Order = 0)] public MongoEventType Type => MongoEventType.Delete;

    [DataMember(Order = 1)] public BsonDocument Key { get; set; } = new();

    [DataMember(Order = 3)] public DateTime ClusterTime { get; set; } = DateTime.UtcNow;
}

[DataContract]
public sealed class ReplaceEvent : IMongoEvent
{
    public ReplaceEvent() {}

    public ReplaceEvent(BsonDocument key, BsonDocument document, DateTime clusterTime) => (Key, Document, ClusterTime) = (key, document, clusterTime);

    [DataMember(Order = 0)] public MongoEventType Type => MongoEventType.Replace;

    [DataMember(Order = 1)] public BsonDocument Key { get; set; } = new();

    [DataMember(Order = 2)] public BsonDocument Document { get; set; } = default!;
    
    [DataMember(Order = 3)] public DateTime ClusterTime { get; set; } = DateTime.UtcNow;
}

[DataContract]
public sealed class UpdateEvent : IMongoEvent
{
    public UpdateEvent() {}

    public UpdateEvent(BsonDocument key, BsonDocument? update, string[]? removedFields, BsonArray? truncatedArrays, BsonDocument? document, DateTime clusterTime)
        : this(key, update, removedFields, truncatedArrays?.Select(v => v.AsBsonDocument).ToArray(), document, clusterTime) { }
    
    public UpdateEvent(BsonDocument key, BsonDocument? update, string[]? removedFields, BsonDocument[]? truncatedArrays, BsonDocument? document, DateTime clusterTime)
    {
        Key = key;
        Document = document;
        ClusterTime = clusterTime;
        Update = update is { ElementCount: > 0 } ? update : null;
        RemovedFields = removedFields is { Length: > 0 } ? removedFields : null;
        TruncatedArrays = truncatedArrays is { Length: > 0 } ? truncatedArrays : null;
    }

    [DataMember(Order = 0)] public MongoEventType Type => MongoEventType.Update;

    [DataMember(Order = 1)] public BsonDocument Key { get; set; } = new();

    [DataMember(Order = 2)] public BsonDocument? Update { get; set; }

    [DataMember(Order = 3)] public string[]? RemovedFields { get; set; }

    [DataMember(Order = 4)] public BsonDocument[]? TruncatedArrays { get; set; }

    [DataMember(Order = 5)] public BsonDocument? Document { get; set; }

    [DataMember(Order = 6)] public DateTime ClusterTime { get; set; } = DateTime.UtcNow;
}

[DataContract]
public sealed class UnsupportedEvent : IMongoEvent
{
    public UnsupportedEvent() {}

    public UnsupportedEvent(string name, DateTime clusterTime) => (Name, ClusterTime) = (name, clusterTime);

    [DataMember(Order = 0)] public MongoEventType Type => MongoEventType.Unsupported;

    [DataMember(Order = 1)] public string Name { get; set; } = string.Empty;

    [DataMember(Order = 2)] public DateTime ClusterTime { get; set; } = DateTime.UtcNow;
}