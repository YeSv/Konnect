
using Konnect.Common;
using Konnect.Mongo;
using Konnect.Pipelines.MM;
using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration.Readers.Pipelines;

public sealed class MmPipelineReader : IConfigurationReader<PipelineFn>
{
    readonly Clusters _mongos;
    readonly IConfigurationReader<Processor>[] _processors;
    readonly IConfigurationReader<Partitioner>[] _partitioners;

    public MmPipelineReader(
        Clusters mongos, 
        IEnumerable<IConfigurationReader<Processor>> processors,
        IEnumerable<IConfigurationReader<Partitioner>> partitioners)
    {
        _mongos = mongos;
        _processors = processors.ToArray();
        _partitioners = partitioners.ToArray();   
    }

    public PipelineFn? Read(IConfigurationSection pipeline)
    {
        if (pipeline.GetTypeOf() != "mm") return null;

        return (obs, token) =>
        {
            var deps = new PipelineDependencies(new
            (
                new Sink(_mongos, pipeline.ReadSinkConfiguration<SinkConfiguration>()),
                new Source(_mongos, pipeline.ReadSourceConfiguration<SourceConfiguration>()),
                new Offsets(_mongos, pipeline.ReadOffsetsConfiguration<OffsetsConfiguration>()),
                pipeline.ReadProcessors(_processors).Compose(),
                pipeline.ReadPartitioner(_partitioners)
            ), pipeline.ReadOptionalConfiguration<PipelineConfiguration>(), obs);

            return Pipeline.Run(deps, token);
        };
    }
}