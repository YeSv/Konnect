using System.Threading.Tasks.Dataflow;
using Konnect.Common.Extensions;
using Confluent.Kafka;

namespace Konnect.Kafka;

public enum OffsetStatus : byte { InProgress, ToBeStored }

public readonly record struct EventStoreMetadata(TopicPartitionOffset Tpo, CancellationToken Lifetime);

public sealed record PartitionMetadata(Dictionary<long, OffsetStatus> Offsets)
{
    public long? MinInProgressOffset { get; set; }
}

public sealed class OffsetsObserver
{
    public Action<Exception> OnProcessingError { get; set; } = e => {};
    public Action<TopicPartitionOffset> OnOffsetFlushed { get; set; } = t => {};
}

public sealed class Offsets : IDisposable
{
    OffsetsObserver? _observer;

    readonly ActionBlock<object?> _processor;
    readonly CancellationTokenSource _switch;
    readonly Dictionary<int, PartitionMetadata> _partitions;
    
    public Offsets()
    {
        _switch = new();
        _partitions = new();
        _processor = new(o =>
        {
            try
            {
                _ = o switch
                {
                    GetSnapshotCommand g => HandleGetSnapshot(g),
                    MarkToBeStoredCommand s => HandleToBeStored(s.Tpo, s.Lifetime),
                    MarkInProgressCommand i => HandleInProgress(i.Tpo, i.Lifetime),
                    RemovePartitionCommand r => HandleRemovePartition(r.Partition),
                    _ => null
                };
            }
            catch (Exception ex)
            {
                _observer?.OnProcessingError(ex);
            }
        }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1, CancellationToken = _switch.Token });
    }

    public Offsets WithObserver(OffsetsObserver obs) 
    {
        _observer = obs;
        return this;
    }

    public Task<Dictionary<int, PartitionMetadata>> GetSnapshot() =>
        new TaskCompletionSource<Dictionary<int, PartitionMetadata>>(TaskCreationOptions.RunContinuationsAsynchronously)
            .Tap(_processor, (c, p) => p.Post(new GetSnapshotCommand(c)))
            .Map(t => t.Task);

    public void RemovePartition(int partition) => _processor.Post(new RemovePartitionCommand(partition));

    public void MarkToBeStored(EventStoreMetadata metadata) => _ = metadata switch
    {
        { Tpo: null } => false,
        { Lifetime.IsCancellationRequested: true } => false,
        { Lifetime: var l, Tpo: var tpo } => _processor.Post(new MarkToBeStoredCommand(tpo, l))
    };

    public void MarkInProgress(EventStoreMetadata metadata) => _ = metadata switch
    {
        { Tpo: null } => false,
        { Lifetime.IsCancellationRequested: true } => false,
        { Lifetime: var l, Tpo: var tpo } => _processor.Post(new MarkInProgressCommand(tpo, l))
    };

    object? HandleRemovePartition(int partition)
    {
        _partitions.Remove(partition, out _);
        return null;
    }

    object? HandleInProgress(TopicPartitionOffset tpo, CancellationToken lifetime)
    {
        var (_, partition, offset) = (tpo.Topic, tpo.Partition.Value, tpo.Offset.Value);

        if (lifetime.IsCancellationRequested) return null;

        if (!_partitions.TryGetValue(partition, out var metadata)) 
            metadata = _partitions[partition] = new(new());

        if (metadata.MinInProgressOffset == null) 
            metadata.MinInProgressOffset = offset; // MinInProgressOffset increases in StoreOffset method

        metadata.Offsets[offset] = OffsetStatus.InProgress;

        return null;
    }

    object? HandleToBeStored(TopicPartitionOffset tpo, CancellationToken lifetime)
    {
        var (_, partition, offset) = (tpo.Topic, tpo.Partition.Value, tpo.Offset.Value);

        if (lifetime.IsCancellationRequested) return null;

        if (!_partitions.TryGetValue(partition, out var metadata)) return null;
        if (metadata.MinInProgressOffset == null) return null; // No messages were registered
        if (metadata.MinInProgressOffset > offset) return null; // Called "StoreOffset" multiple times
        if (!metadata.Offsets.ContainsKey(offset)) return null; // Unknown offset

        metadata.Offsets[offset] = OffsetStatus.ToBeStored;
        if (metadata.MinInProgressOffset != offset) return null;

        while (metadata.Offsets.TryGetValue(metadata.MinInProgressOffset!.Value, out var status) && status == OffsetStatus.ToBeStored) 
            metadata.Offsets.Remove(metadata.MinInProgressOffset++.Value);

        var nextOffset = metadata.MinInProgressOffset.Value;

        if (metadata.Offsets.Count == 0) metadata.MinInProgressOffset = null;

        _observer?.OnOffsetFlushed.Invoke(new(tpo.TopicPartition, nextOffset));

        return null;
    }

    object? HandleGetSnapshot(GetSnapshotCommand cmd)
    {
        var snapshot = default(Dictionary<int, PartitionMetadata>);
        try
        {
            snapshot = _partitions.ToDictionary(
                p => p.Key, 
                p => new PartitionMetadata(p.Value.Offsets.ToDictionary(d => d.Key, d => d.Value)) { MinInProgressOffset = p.Value.MinInProgressOffset  });
        }
        finally
        {
            cmd.Completion.TrySetResult(snapshot ?? new());
        }

        return snapshot;
    }

    public void Dispose()
    {
        _switch?.Cancel();
        _processor?.Complete();
        _processor?.Completion.ContinueWith(t => 
        {
            if (t.IsCanceled || t.IsCompletedSuccessfully) return;
            throw t.Exception!;
        }).Wait();
    }

    sealed record MarkToBeStoredCommand(TopicPartitionOffset Tpo, CancellationToken Lifetime);
    sealed record MarkInProgressCommand(TopicPartitionOffset Tpo, CancellationToken Lifetime);
    sealed record RemovePartitionCommand(int Partition);
    sealed record GetSnapshotCommand(TaskCompletionSource<Dictionary<int, PartitionMetadata>> Completion);
}