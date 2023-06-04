using Confluent.Kafka;
using Konnect.Common.Extensions;
using Simple.Dotnet.Utilities.Buffers;

namespace Konnect.Kafka;

public readonly record struct Event(ConsumeResult<byte[], byte[]> Result, CancellationToken Lifetime);

public sealed class Source : IDisposable
{
    SourceObserver? _observer;
    
    readonly SourceConfiguration _configuration;
    readonly Lazy<IConsumer<byte[], byte[]>> _consumer;
    readonly Dictionary<int, CancellationTokenSource> _lifetimes = new();

    public Source(IEnumerable<KafkaClusterConfiguration> clusters, SourceConfiguration configuration)
    {
        _configuration = configuration;
        
        var cluster = clusters.First(c => c.Name == _configuration.Connection.Type);
        _consumer = new(() => CreateConsumer(AssembleConfiguration(cluster.Config, _configuration.Connection.Config)));
    }

    public Source WithObserver(SourceObserver observer) 
    {
        _observer = observer;
        return this;
    }

    public void StoreOffset(TopicPartitionOffset tpo) => _consumer.Value.StoreOffset(tpo);

    public Rent<Event> Read(CancellationToken token)
    {
        using var batch = new Rent<Event>(_configuration.BatchSize);

        while (!batch.IsFull)
        {
            token.ThrowIfCancellationRequested();
            
            var result = _consumer.Value.Consume(_configuration.ConsumeDuration);

            if (result is null && batch.HasSome) break;
            if (result is null or { IsPartitionEOF: true }) continue;

            batch.Append(new(result, _lifetimes[result.Partition].Token));
        }

        return batch.Clone();
    }

    public void Dispose()
    {
        if (!_consumer.IsValueCreated) return;

        try
        {
            _consumer.Value.Close();
            _consumer.Value.Dispose();
        }
        finally
        {
            foreach (var p in _lifetimes) p.Value.Cancel();
        }
    }

    IConsumer<byte[], byte[]> CreateConsumer(IDictionary<string, string> config) 
    {
        var builder = new ConsumerBuilder<byte[], byte[]>(config);
        builder.SetPartitionsAssignedHandler((c, t) => 
        {
            foreach (var p in t) _lifetimes[p.Partition] = new();
            _observer?.OnAssigned(c, t);
        });
        builder.SetPartitionsRevokedHandler((c, t) => 
        {
            foreach (var p in t) _lifetimes.Tap(p.Partition, (ps, p) => ps[p].Cancel()).Tap(p.Partition, (ps, p) => ps.Remove(p));
            _observer?.OnRevoked(c, t);
        });

        if (_observer?.OnError != null) builder.SetErrorHandler((c, e) => _observer.OnError(c, e));
        if (_observer?.OnStatistics != null) builder.SetStatisticsHandler((c, s) => _observer.OnStatistics(c, s));
        if (_observer?.OnLog != null) builder.SetLogHandler((c, l) => _observer.OnLog(c, l));

        return builder
            .Build()
            .Tap(_configuration, (c, cfg) => c.Subscribe(cfg.Connection.Topics));
    }

    static Dictionary<string, string> AssembleConfiguration(Dictionary<string, string> environment, Dictionary<string, string> source)
    {
        var config = environment.ToDictionary(v => v.Key, v => v.Value);
        foreach (var (key, value) in DefaultConfiguration) config[key] = value;
        foreach (var (key, value) in source) config[key] = value;
        return config;
    }

    static readonly Dictionary<string, string> DefaultConfiguration = new()
    {
        ["enable.auto.commit"] = "true",
        ["auto.offset.reset"] = "latest",
        ["auto.commit.interval.ms"] = "5000",
        ["statistics.interval.ms"] = "10000",
        ["enable.auto.offset.store"] = "false"
    };
}