
using Konnect.Kafka;
using Konnect.Mongo;
using Konnect.Pipelines.KM;
using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration.Readers.Pipelines;

public sealed class KmPipelineReader : IConfigurationReader<PipelineFn>
{
    readonly Clusters _mongos;
    readonly IEnumerable<KafkaClusterConfiguration> _kafkas;
    readonly IConfigurationReader<Kafka.Processor>[] _preProcessors;
    readonly IConfigurationReader<Kafka.Partitioner>[] _partitioners;
    readonly IConfigurationReader<Mongo.Processor>[] _postProcessors;

    public KmPipelineReader(
        Clusters mongos,
        IEnumerable<KafkaClusterConfiguration> kafkas, 
        IEnumerable<IConfigurationReader<Kafka.Processor>> preProcessors,
        IEnumerable<IConfigurationReader<Mongo.Processor>> postProcessors,
        IEnumerable<IConfigurationReader<Kafka.Partitioner>> partitioners)
    {
        _mongos = mongos;
        _kafkas = kafkas;
        _partitioners = partitioners.ToArray();
        _preProcessors = preProcessors.ToArray();
        _postProcessors = postProcessors.ToArray();
    }

    public PipelineFn? Read(IConfigurationSection pipeline)
    {
        if (pipeline.GetTypeOf() != "km") return null;
        
        return (obs, token) =>
        {
            var deps = new PipelineDependencies(new
            (
                new Mongo.Sink(_mongos, pipeline.ReadSinkConfiguration<Mongo.SinkConfiguration>()),
                new Kafka.Source(_kafkas, pipeline.ReadSourceConfiguration<Kafka.SourceConfiguration>()),
                pipeline.ReadPreProcessors(_preProcessors).Compose(),
                pipeline.ReadPostProcessors(_postProcessors).Compose(),
                pipeline.ReadPartitioner(_partitioners)
            ), pipeline.ReadOptionalConfiguration<PipelineConfiguration>(), obs);

            return Pipeline.Run(deps, token);
        };
    }
}