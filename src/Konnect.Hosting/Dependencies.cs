
using App.Metrics;

using Konnect.Common;
using Konnect.Kafka;
using Konnect.Mongo;

using Microsoft.Extensions.Logging;

namespace Konnect.Hosting.Dependencies;

public sealed record Observability(ILoggerFactory Logger, IMetrics Metrics)
{
    public ObservabilityContext Context => new(Metrics, Logger);
}

public sealed record Storages(Clusters Mongos, IEnumerable<KafkaClusterConfiguration> Kafkas);