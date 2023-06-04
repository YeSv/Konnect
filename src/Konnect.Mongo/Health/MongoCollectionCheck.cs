using Konnect.Common.Health;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Konnect.Mongo.Health;

public sealed class MongoCollectionsCheckConfiguration
{
    public bool Ignore { get; set; }
    public string? Name { get; set; }
    public string Type { get; set; } = string.Empty;
    public string Database { get; set; } = string.Empty;
    public string[] Collections { get; set; } = Array.Empty<string>();
}

public sealed class MongoCollectionsCheck
{
    readonly IMongoDatabase _database;
    readonly ListCollectionNamesOptions _options;
    readonly MongoCollectionsCheckConfiguration _configuration;

    public MongoCollectionsCheck(Clusters clusters, MongoCollectionsCheckConfiguration config)
    {

        _configuration = config;
        _options = CreateOptions(_configuration.Collections);
        _database = clusters.Get(config.Type).GetDatabase(config.Database);
    }

    public async Task<Result> Check(CancellationToken token)
    {
        var (ignore, name) = (_configuration.Ignore, _configuration.Name ?? $"mongo_{_configuration.Type}_{_configuration.Name}");
        try
        {
            var collections = await (await _database.ListCollectionNamesAsync(_options, token)).ToListAsync(token);
            return new(name, ignore, collections switch
            {
                null => HealthCheckResult.Unhealthy($"Collections were not returned for {_configuration.Type} cluster"),
                { Count: var count } when count == _configuration.Collections.Length => HealthCheckResult.Healthy(),
                _ => HealthCheckResult.Unhealthy($"Collections count does not match for {_configuration.Type} cluster. Returned: {string.Join(",", collections)}")
            });
        }
        catch (Exception ex)
        {
            return new(name, ignore, new(HealthStatus.Unhealthy, "An error occurred", ex));
        }
    }

    static ListCollectionNamesOptions CreateOptions(string[] collections) => new()
    {
        Filter = new BsonDocument
        {
            ["name"] = new BsonDocument 
            {
                ["$in"] = new BsonArray(collections)
            }
        }
    };
}