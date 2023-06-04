using Konnect.Common.Health;
using Confluent.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Konnect.Kafka.Health;

public sealed class KafkaClusterCheckConfiguration
{
    public bool Ignore { get; set; }
    public string? Name { get; set; }
    public string Type { get; set; } = string.Empty;
    public string Topic { get; set; } = "konnect-health-checks";
}

public sealed class KafkaClusterCheck
{
    readonly IProducer<byte[], byte[]> _producer;
    readonly KafkaClusterCheckConfiguration _configuration;

    public KafkaClusterCheck(IEnumerable<KafkaClusterConfiguration> clusters, KafkaClusterCheckConfiguration config)
    {
        _configuration = config;
        _producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig 
        {
            BootstrapServers = clusters.First(c => c.Name == _configuration.Type).Config["bootstrap.servers"]
        }).Build();
    }

    public async Task<Result> Check(CancellationToken token)
    {
        var (ignore, name) = (_configuration.Ignore, _configuration.Name ?? $"kafka_{_configuration.Type}");
        try
        {
            await _producer.ProduceAsync(_configuration.Topic, new(), token);
            return new(name, ignore, HealthCheckResult.Healthy());
        }
        catch (Exception ex)
        {
            return new(name, ignore, HealthCheckResult.Unhealthy("An error occurred", ex));
        }
    }
}