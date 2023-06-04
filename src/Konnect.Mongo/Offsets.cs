using MongoDB.Bson;
using MongoDB.Driver;
using System.Threading.Tasks.Dataflow;
using Konnect.Common.Extensions;

namespace Konnect.Mongo;

public sealed class OffsetsConfiguration
{
    public string Id { get; set; } = string.Empty; // Unique offsets identifier
    public ConnectionConfiguration Connection { get; set; } = new();
    public string OffsetsCollection { get; set;} = "KonnectOffsets";
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(10);
}

public sealed class OffsetsObserver
{
    public Action OnFlush { get; set; } = () => {};
    public Action<Exception> OnFlushError { get; set; } = e => {};
}

// Stores offsets in memory per collection
public sealed class Offsets : IDisposable
{
    enum OffsetStatus : byte { InProgress, ToBeStored }
    readonly record struct OffsetState(Offset Offset, OffsetStatus Status, long Order);

    static readonly ReplaceOptions Upsert = new() { IsUpsert = true };
    static readonly long EmptyOffset = -1;

    OffsetsObserver _observer = new();
    long _inProgressOffset = EmptyOffset;

    readonly Timer _timer;
    readonly BsonDocument _filter;
    readonly CancellationTokenSource _switch;
    readonly ActionBlock<OffsetState?> _processor;
    readonly IMongoCollection<BsonDocument> _collection;

    readonly Dictionary<long, OffsetState> _offsets = new();
    
    public Offsets(Clusters clusters, OffsetsConfiguration config) 
    {
        _switch = new();
        _filter = new() { ["_id"] = config.Id };
        _collection = clusters.Get(config.Connection.Type).GetDatabase(config.Connection.Database).GetCollection<BsonDocument>(config.OffsetsCollection);

        _processor = new(async o => 
        {
            try
            {
                if (o is not null)
                {
                    MarkInMemory(o.Value);
                    return;
                }

                var offsetToCommit = GetNextOffsetToCommit();
                if (offsetToCommit is null) return;

                await offsetToCommit
                    .Tap(_observer, (_, o) => o.OnFlush())
                    .Map(o => _collection.ReplaceOneAsync(_filter, OffsetToDoc(o!.Value, config.Id), Upsert, _switch.Token));
            }
            catch (OperationCanceledException) {}
            catch (Exception ex)
            {
                _observer.OnFlushError(ex);
            }
        }, new() { CancellationToken = _switch.Token, MaxDegreeOfParallelism = 1 });

        _timer = new(_ => _processor.Post(default), default, config.FlushInterval, config.FlushInterval);
    }

    public Offsets WithObserver(OffsetsObserver observer) => (_observer = observer).Map(this, (o, t) => t);

    public void InProgress(Event @event) => _processor.Post(new(@event.Offset, OffsetStatus.InProgress, @event.Order));

    public void StoreOffset(Event @event) => _processor.Post(new(@event.Offset, OffsetStatus.ToBeStored, @event.Order));

    public async Task<Offset?> GetCommittedOffset(CancellationToken token = default) =>
        (await _collection.Find(_filter).FirstOrDefaultAsync(token)) switch
        {
            null => null,
            var d => new(d["token"].AsBsonDocument, d["timestamp"].AsBsonTimestamp)
        };

    public Task DeleteOffset(CancellationToken token = default) => _collection.DeleteOneAsync(_filter, token);

    public void Dispose()
    {
        _switch?.Cancel();
        _timer?.Dispose();
        _processor?.Complete();
    }

    void MarkInMemory(OffsetState state)
    {
        var key = state.Order;

        if (key < _inProgressOffset) return;
        if (_offsets.TryGetValue(key, out var existing) && state.Status == OffsetStatus.InProgress) return;

        _offsets[key] = state;
        if (_inProgressOffset == EmptyOffset) _inProgressOffset = key; 
    }

    Offset? GetNextOffsetToCommit()
    {
        if (_inProgressOffset == EmptyOffset) return null;
        
        var offsetToCommit = default(Offset?);
        while (_offsets.TryGetValue(_inProgressOffset, out var state) && state.Status == OffsetStatus.ToBeStored) 
        {
            offsetToCommit = state.Offset;
            _offsets.Remove(_inProgressOffset++);
        }

        if (_offsets.Count == 0) _inProgressOffset = EmptyOffset;
        
        return offsetToCommit;
    }

    static BsonDocument OffsetToDoc(Offset offset, string id) => new()
    {
        ["_id"] = id,
        ["token"] = offset.Token,
        ["writtenAt"] = DateTime.UtcNow,
        ["timestamp"] = offset.Timestamp
    };
}