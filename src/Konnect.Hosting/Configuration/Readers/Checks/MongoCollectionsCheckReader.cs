
using Konnect.Common.Health;
using Konnect.Mongo;
using Konnect.Mongo.Health;
using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration.Readers.Checks;

public sealed class MongoCollectionsCheckReader : IConfigurationReader<CheckFn>
{
    readonly Clusters _mongos;
    
    public MongoCollectionsCheckReader(Clusters mongos) => _mongos = mongos;

    public CheckFn? Read(IConfigurationSection section)
    {
        var type = section.GetTypeOf();
        if (type != "mongo-collections") return null;

        var check = new MongoCollectionsCheck(_mongos, section.ReadConfiguration<MongoCollectionsCheckConfiguration>()!);
        return token => check.Check(token);
    }
}