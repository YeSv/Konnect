using Confluent.Kafka;
using Simple.Dotnet.Utilities.Buffers;

namespace Konnect.Kafka;

public sealed class Sink : IDisposable
{
    SinkObserver? _observer;

    readonly SinkConfiguration _configuration;
    readonly Lazy<IProducer<byte[], byte[]>> _producer;

    public Sink(IEnumerable<KafkaClusterConfiguration> clusters, SinkConfiguration configuration)
    {
        _configuration = configuration;
        
        var cluster = clusters.First(c => c.Name == _configuration.Connection.Type);
        _producer = new(() => CreateProducer(AssembleConfiguration(cluster.Config, _configuration.Connection.Config)));
    }

    public Sink WithObserver(SinkObserver observer) 
    {
        _observer = observer;
        return this;
    }
    
    public IProducer<byte[], byte[]> UnwrapUnsafe() => _producer.Value;

    public async Task Write(ReadOnlyMemory<Message<byte[], byte[]>> messages, CancellationToken token)
    {
        using var tasks = new Rent<Task<DeliveryResult<byte[], byte[]>>>(messages.Length);
        
        for (var i = 0; i < messages.Span.Length; i++) 
            tasks.Append(_producer.Value.ProduceAsync(_configuration.Connection.Topics[0], messages.Span[i], token));

        await Task.WhenAll(tasks.AsEnumerable());

        for (var i = 0; i < tasks.Length; i++)
        {
            var result = tasks.WrittenSpan[i].GetAwaiter().GetResult();
            if (result != null && result.Status != PersistenceStatus.Persisted) throw new Exception("Message was not persisted on produce");
        }
    }

    public void Dispose()
    {
        if (_producer.IsValueCreated) _producer.Value.Dispose();
    }

    IProducer<byte[], byte[]> CreateProducer(Dictionary<string, string> config) 
    {
        var builder = new ProducerBuilder<byte[], byte[]>(config);
        if (_observer?.OnError != null) builder.SetErrorHandler((p, e) => _observer.OnError(p, e));
        if (_observer?.OnStatistics != null) builder.SetStatisticsHandler((p, s) => _observer.OnStatistics(p, s));
        if (_observer?.OnLog != null) builder.SetLogHandler((p, l) => _observer.OnLog(p, l));

        return builder.Build();   
    }

    static Dictionary<string, string> AssembleConfiguration(Dictionary<string, string> environment, Dictionary<string, string> sink)
    {
        var config = environment.ToDictionary(v => v.Key, v => v.Value);
        foreach (var (key, value) in DefaultConfiguration) config[key] = value;
        foreach (var (key, value) in sink) config[key] = value;
        return config;
    }

    static readonly Dictionary<string, string> DefaultConfiguration = new()
    {
        ["acks"] = "all",
        ["retries"] = "3",
        ["linger.ms"] = "250",
        ["retry.backoff.ms"] = "1000",
        ["batch.num.messages"] = "50000",
        ["queue.buffering.max.messages"] = "300000"
    };
}
