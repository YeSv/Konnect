using Konnect.Mongo.Contracts;

namespace Konnect.Mongo;

public delegate int Partitioner(IMongoEvent? @event, int partitions);