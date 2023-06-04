using Konnect.Common.Extensions;
using Konnect.Hosting.Configuration;
using Konnect.Hosting.Configuration.Readers;
using Konnect.Hosting.Configuration.Readers.Actions;
using Konnect.Hosting.Configuration.Readers.Checks;
using Konnect.Hosting.Configuration.Readers.Partitioners;
using Konnect.Hosting.Configuration.Readers.Pipelines;
using Konnect.Hosting.Configuration.Readers.Processors;
using Konnect.Hosting.Health;
using Konnect.Kafka;
using Konnect.Mongo;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Konnect.Hosting;

public static class Services
{
    public static IServiceCollection AddKonnectDependencies(this IServiceCollection services, IConfigurationRoot config) => 
        services
            .AddKafkas(config)
            .AddMongos(config)
            .AddConfigReaders()
            .AddSingleton<KonnectHealth>()
            .AddSingleton<IConfiguration>(config)
            .AddSingleton<Dependencies.Storages>()
            .AddSingleton<Dependencies.Observability>();

    public static IServiceCollection AddKafkas(this IServiceCollection services, IConfigurationRoot config)
    {
        var storages = config.GetInstanceConfiguration().GetStoragesSection();
        
        foreach (var cluster in storages.ReadKafkas())
            services.AddSingleton(config.GetSection($"kafkas:{cluster}").Read<KafkaClusterConfiguration>().Tap(c => c.Name ??= cluster)!);

        return services;
    }

    public static IServiceCollection AddMongos(this IServiceCollection services, IConfigurationRoot config)
    {
        var storages = config.GetInstanceConfiguration().GetStoragesSection();

        foreach (var cluster in storages.ReadMongos())
            services.AddSingleton(config.GetSection($"mongos:{cluster}").Read<MongoClusterConfiguration>().Tap(c => c.Name ??= cluster)!);

        return services.AddSingleton<Clusters>();
    }

    public static IServiceCollection AddConfigReaders(this IServiceCollection services) => 
        services
            .AddSingleton<IConfigurationReader<ActionFn>, ActionReader>()

            .AddSingleton<IConfigurationReader<CheckFn>, KafkaClusterCheckReader>()
            .AddSingleton<IConfigurationReader<CheckFn>, MongoCollectionsCheckReader>()

            .AddSingleton<IConfigurationReader<PipelineFn>, MkPipelineReader>()
            .AddSingleton<IConfigurationReader<PipelineFn>, KkPipelineReader>()
            .AddSingleton<IConfigurationReader<PipelineFn>, KmPipelineReader>()
            .AddSingleton<IConfigurationReader<PipelineFn>, MmPipelineReader>()

            .AddSingleton<IConfigurationReader<Kafka.Partitioner>, MessageKeyPartitionerReader>()
            .AddSingleton<IConfigurationReader<Mongo.Partitioner>, ObjectIdKeyPartitionerReader>()

            .AddSingleton<IConfigurationReader<Mongo.Processor>, MongoEventTypesProcessorReader>()
            .AddSingleton<IConfigurationReader<Mongo.Processor>, MongoExcludeFieldsProcessorReader>();
}