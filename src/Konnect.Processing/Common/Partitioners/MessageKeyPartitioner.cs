using Konnect.Common.Extensions;
using Confluent.Kafka;
using Simple.Dotnet.Utilities.Partitioners;

namespace Konnect.Processing.Common.Partitioners;

public static class MessageKeyPartitioner
{
    public static Kafka.Partitioner Lambda => GetPartition;

    public static int GetPartition(this Message<byte[], byte[]>? message, int partitions) => message switch
    {
        null => 0,
        { Key: null } => 0,
        { Key: { Length: 0 } } => 0,
        { Key: var k } => HashPartitioner.GetPartition(new Bytes(k), partitions)
    };
}