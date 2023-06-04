using Konnect.Kafka;
using Konnect.Mongo;
using Konnect.Pipelines.MK;
using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration.Readers.Pipelines;

public sealed class MkPipelineReader : IConfigurationReader<PipelineFn>
{
    readonly Clusters _mongos;
    readonly IEnumerable<KafkaClusterConfiguration> _kafkas;
    readonly IConfigurationReader<Mongo.Processor>[] _preProcessors;
    readonly IConfigurationReader<Kafka.Processor>[] _postProcessors;
    readonly IConfigurationReader<Mongo.Partitioner>[] _partitioners;

    public MkPipelineReader(
        Clusters mongos,
        IEnumerable<KafkaClusterConfiguration> kafkas, 
        IEnumerable<IConfigurationReader<Mongo.Partitioner>> partitioners,
        IEnumerable<IConfigurationReader<Mongo.Processor>> preProcessors,
        IEnumerable<IConfigurationReader<Kafka.Processor>> postProcessors)
    {
        _mongos = mongos;
        _kafkas = kafkas;
        _partitioners = partitioners.ToArray();
        _preProcessors = preProcessors.ToArray();
        _postProcessors = postProcessors.ToArray();
    }

    public PipelineFn? Read(IConfigurationSection pipeline)
    {
        if (pipeline.GetTypeOf() != "mk") return null;

        return (obs, token) =>
        {
            var deps = new PipelineDependencies(new
            (
                new Kafka.Sink(_kafkas, pipeline.ReadSinkConfiguration<Kafka.SinkConfiguration>()),
                new Mongo.Source(_mongos, pipeline.ReadSourceConfiguration<Mongo.SourceConfiguration>()),
                new Mongo.Offsets(_mongos, pipeline.ReadOffsetsConfiguration<Mongo.OffsetsConfiguration>()),
                pipeline.ReadPreProcessors(_preProcessors).Compose(),
                pipeline.ReadPostProcessors(_postProcessors).Compose(),
                pipeline.ReadPartitioner(_partitioners)
            ), pipeline.ReadOptionalConfiguration<PipelineConfiguration>(), obs);

            return Pipeline.Run(deps, token);
        };
    }
}