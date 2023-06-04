using Konnect.Mongo;
using Konnect.Processing.Common.Partitioners;
using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration.Readers.Partitioners;

public sealed class ObjectIdKeyPartitionerReader : IConfigurationReader<Partitioner>
{
    public Partitioner? Read(IConfigurationSection section) => ObjectIdKeyPartitioner.Lambda;
}