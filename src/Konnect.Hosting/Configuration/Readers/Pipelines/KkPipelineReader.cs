using Konnect.Kafka;
using Konnect.Pipelines.KK;

using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration.Readers.Pipelines;

public sealed class KkPipelineReader : IConfigurationReader<PipelineFn>
{
    readonly IEnumerable<KafkaClusterConfiguration> _kafkas;
    readonly IConfigurationReader<Processor>[] _processors;
    readonly IConfigurationReader<Partitioner>[] _partitioners;

    public KkPipelineReader(
        IEnumerable<KafkaClusterConfiguration> kafkas, 
        IEnumerable<IConfigurationReader<Processor>> processors,
        IEnumerable<IConfigurationReader<Partitioner>> partitioners)
    {
        _kafkas = kafkas;
        _processors = processors.ToArray();
        _partitioners = partitioners.ToArray();   
    }

    public PipelineFn? Read(IConfigurationSection pipeline)
    {
        if (pipeline.GetTypeOf() != "kk") return null;

        return (obs, token) =>
        {
            var deps = new PipelineDependencies(new
            (
                new Sink(_kafkas, pipeline.ReadSinkConfiguration<SinkConfiguration>()),
                new Source(_kafkas, pipeline.ReadSourceConfiguration<SourceConfiguration>()),
                pipeline.ReadProcessors(_processors).Compose(),
                pipeline.ReadPartitioner(_partitioners)
            ), pipeline.ReadOptionalConfiguration<PipelineConfiguration>(), obs);

            return Pipeline.Run(deps, token);
        };
    }
}