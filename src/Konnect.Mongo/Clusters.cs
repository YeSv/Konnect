using MongoDB.Driver;

namespace Konnect.Mongo;

public sealed class Clusters
{
    readonly Dictionary<string, MongoClient> _clusters;

    public Clusters(IEnumerable<MongoClusterConfiguration> clusters) =>
        _clusters = clusters.ToDictionary(c => c.Name, c => new MongoClient(c.ConnectionString));

    public MongoClient Get(string name) => _clusters[name];
}
