using Konnect.Kafka;
using Konnect.Kafka.Health;
using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration.Readers.Checks;

public sealed class KafkaClusterCheckReader : IConfigurationReader<CheckFn>
{
    readonly IEnumerable<KafkaClusterConfiguration> _kafkas;

    public KafkaClusterCheckReader(IEnumerable<KafkaClusterConfiguration> kafkas) => _kafkas = kafkas;

    public CheckFn? Read(IConfigurationSection section)
    {
        var type = section.GetTypeOf();
        if (type != "kafka-cluster") return null;

        var check = new KafkaClusterCheck(_kafkas, section.ReadConfiguration<KafkaClusterCheckConfiguration>());
        return token => check.Check(token);
    }
}