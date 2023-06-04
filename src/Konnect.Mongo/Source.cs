using MongoDB.Bson;
using MongoDB.Driver;
using Konnect.Mongo.Contracts;
using Simple.Dotnet.Utilities.Buffers;

namespace Konnect.Mongo;

public readonly record struct Event(Offset Offset, IMongoEvent MongoEvent, long Order);
public readonly record struct Offset(BsonDocument Token, BsonTimestamp Timestamp);

public sealed class Source : IDisposable
{
    long _eventsOrder = 0;

    readonly SourceConfiguration _configuration;
    readonly IMongoCollection<BsonDocument> _collection;
    readonly Lazy<IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>> _cursor;

    public Source(Clusters clusters, SourceConfiguration configuration)
    {
        _configuration = configuration;
        _collection = clusters
            .Get(_configuration.Connection.Type)
            .GetDatabase(_configuration.Connection.Database)
            .GetCollection<BsonDocument>(configuration.ChangeStream.Collection);

        _cursor = new(() => CreateStream(_collection, _configuration.ChangeStream), true);
    }

    public IMongoCollection<BsonDocument> UnwrapUnsafe() => _collection;

    public Source StartFrom(Offset offset)
    {
        _configuration.ChangeStream.Token ??= offset.Token?.ToString();
        _configuration.ChangeStream.Timestamp ??= offset.Timestamp?.Value;
        return this;
    }

    public async Task<Rent<Event>> Read(CancellationToken token)
    {
        var hasSome = await _cursor.Value.MoveNextAsync(token);
        if (!hasSome) return new(0);

        var events = new Rent<Event>(_configuration.ChangeStream.BatchSize);
        foreach (var streamEvent in _cursor.Value.Current) events.Append(ExtractEvent(streamEvent, _eventsOrder++));

        return events switch
        {
            { HasSome: false } => new(0),
            var e => e
        };
    }

    public void Dispose()
    {
        if (_cursor.IsValueCreated) _cursor.Value.Dispose();
    }

    static Event ExtractEvent(ChangeStreamDocument<BsonDocument> doc, long order) =>
        new(new(doc.ResumeToken, doc.ClusterTime), doc.OperationType switch
        {
            ChangeStreamOperationType.Drop => DropEvent.Shared,
            ChangeStreamOperationType.Invalidate => InvalidateEvent.Shared,
            ChangeStreamOperationType.Delete => new DeleteEvent(doc.DocumentKey, MapTimestamp(doc.ClusterTime)),
            ChangeStreamOperationType.Insert => new InsertEvent(doc.DocumentKey, doc.FullDocument, MapTimestamp(doc.ClusterTime)),
            ChangeStreamOperationType.Replace => new ReplaceEvent(doc.DocumentKey, doc.FullDocument, MapTimestamp(doc.ClusterTime)),
            ChangeStreamOperationType.Update => new UpdateEvent(doc.DocumentKey, doc.UpdateDescription.UpdatedFields, doc.UpdateDescription.RemovedFields, doc.UpdateDescription.TruncatedArrays, doc.FullDocument, MapTimestamp(doc.ClusterTime)),
            _ => new UnsupportedEvent(doc.OperationType.ToString(), MapTimestamp(doc.ClusterTime))
        }, order);

    static IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> CreateStream(IMongoCollection<BsonDocument> collection, ChangeStreamConfiguration opts)
    {
        var streamOptions = new ChangeStreamOptions();

        streamOptions.BatchSize = opts.BatchSize;
        streamOptions.FullDocument = opts.FullDocument ? ChangeStreamFullDocumentOption.UpdateLookup : ChangeStreamFullDocumentOption.Default;
        
        if (opts is { Mode: ChangeStreamMode.UseToken, Token: not null }) streamOptions.StartAfter = BsonDocument.Parse(opts.Token);
        if (opts is { Mode: ChangeStreamMode.FromTimestamp, Timestamp: not null }) streamOptions.StartAtOperationTime = new(opts.Timestamp.Value);
        
        return opts.Pipeline switch
        {
            null or { Length: 0 } => collection.Watch(streamOptions),
            { Length: > 0 } pipeline => collection.Watch(PipelineDefinition<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>>.Create(pipeline), streamOptions)
        };
    }

    static DateTime MapTimestamp(BsonTimestamp timestamp) => DateTime.UnixEpoch.AddSeconds(timestamp.Timestamp);
}