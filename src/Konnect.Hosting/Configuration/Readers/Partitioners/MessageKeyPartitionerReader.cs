using Konnect.Kafka;
using Konnect.Processing.Common.Partitioners;
using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration.Readers.Partitioners;

public sealed class MessageKeyPartitionerReader : IConfigurationReader<Partitioner>
{
    public Partitioner? Read(IConfigurationSection section) => MessageKeyPartitioner.Lambda;
}