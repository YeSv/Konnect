using Confluent.Kafka;

namespace Konnect.Kafka;

public delegate int Partitioner(Message<byte[], byte[]>? message, int partitions);