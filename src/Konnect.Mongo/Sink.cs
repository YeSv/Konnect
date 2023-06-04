using MongoDB.Bson;
using MongoDB.Driver;
using Simple.Dotnet.Utilities.Buffers;
using Konnect.Mongo.Contracts;
using Konnect.Common.Extensions;

namespace Konnect.Mongo;

public sealed class Sink
{
    static readonly BulkWriteOptions Ordered = new BulkWriteOptions { IsOrdered = true };
    static readonly BulkWriteOptions Unordered = new BulkWriteOptions { IsOrdered = false };
    
    readonly SinkConfiguration _configuration;
    readonly IMongoCollection<BsonDocument> _collection;

    public Sink(Clusters clusters, SinkConfiguration configuration)
    {
        _configuration = configuration;
        _collection = clusters.Get(configuration.Connection.Type).GetDatabase(configuration.Connection.Database).GetCollection<BsonDocument>(configuration.Collection);
    }

    public async Task<BulkWriteResult?> Write(ReadOnlyMemory<IMongoEvent> events, CancellationToken token) 
    {
        try
        {
            using var operations = new Rent<WriteModel<BsonDocument>>(events.Length);
            for (var i = 0; i < events.Length; i++)
            {
                var operation = WriteModel(events.Span[i], _configuration.Upserts);
                if (operation != null) operations.Append(operation);
            }

            if (operations.Written == 0) return null;

            var ordering = _configuration.IsOrdered ? Ordered : Unordered;
            return await _collection.BulkWriteAsync(operations.AsEnumerable(), ordering, token);
        }
        catch (MongoBulkWriteException ex)
        {
            if (!_configuration.IsOrdered && ex.WriteErrors?.All(e => e.Code == 11000) is true) return null;
            throw new Exception("Bulk write exception occurred in sink. To skip duplicate errors use IsOrdered = false", ex);
        }
    }

    public IMongoCollection<BsonDocument> UnwrapUnsafe() => _collection;

    static WriteModel<BsonDocument>? WriteModel(IMongoEvent @event, UpsertsConfiguration upserts) => @event switch
    {
        DeleteEvent d => new DeleteOneModel<BsonDocument>(d.Key),
        UpdateEvent u when u.Document != null => new ReplaceOneModel<BsonDocument>(u.Key!, u.Document) { IsUpsert = upserts.UpsertOnUpdate },
        UpdateEvent u => new UpdateOneModel<BsonDocument>(u.Key, UpdateDefinition(u)) { IsUpsert = upserts.UpsertOnUpdate },
        ReplaceEvent r => new ReplaceOneModel<BsonDocument>(r.Key, r.Document) { IsUpsert = upserts.UpsertOnReplace },
        InsertEvent i when upserts.InsertAsReplace => (WriteModel<BsonDocument>)new ReplaceOneModel<BsonDocument>(i.Key!, i.Document) { IsUpsert = true },
        InsertEvent i => (WriteModel<BsonDocument>)new InsertOneModel<BsonDocument>(i.Document),
        _ => null
    };

    static BsonDocument UpdateDefinition(UpdateEvent u)
    {
        var definition = new BsonDocument();
        if (u.Update is { ElementCount: > 0 }) definition["$set"] = u.Update;

        if (u.RemovedFields is { Length: > 0 }) 
            definition["$unset"] = u.RemovedFields.Aggregate(
                new BsonDocument(), 
                (acc, field) => acc.Tap(field, (a, f) => a[f] = string.Empty));

        if (u.TruncatedArrays is { Length: > 0 }) 
            definition["$push"] = u.TruncatedArrays.Aggregate(
                new BsonDocument(), 
                (acc, trunc) => acc.Tap(trunc, (a, t) => a[t["field"].AsString] = new BsonDocument
                {
                    ["$each"] = new BsonArray(),
                    ["$slice"] = t["newSize"].AsInt32
                }));

        return definition;
    }
}
