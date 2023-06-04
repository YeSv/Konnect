
namespace Konnect.Pipelines.Dataflow;

public readonly record struct Partitioned<T>(T Data, int Partition);