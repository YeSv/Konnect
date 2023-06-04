using Konnect.Mongo;
using Konnect.Mongo.Contracts;
using Simple.Dotnet.Utilities.Partitioners;

namespace Konnect.Processing.Common.Partitioners;

public static class ObjectIdKeyPartitioner
{
    public static Partitioner Lambda => GetPartition;

    public static int GetPartition(this IMongoEvent? @event, int partitions) => @event?.GetKey() switch
    {
        null => 0,
        { } k when !k.Contains("_id") || !k["_id"].IsObjectId => 0,
        var k => HashPartitioner.GetPartition(k["_id"].AsObjectId, partitions)
    };
}