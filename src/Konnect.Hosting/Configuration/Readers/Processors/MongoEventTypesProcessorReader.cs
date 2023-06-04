
using Konnect.Hosting.Dependencies;
using Konnect.Mongo;
using Konnect.Processing.Common.Processors;
using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration.Readers.Processors;

public sealed class MongoEventTypesProcessorReader : IConfigurationReader<Processor>
{
    readonly Observability _obs;

    public MongoEventTypesProcessorReader(Observability obs) => _obs = obs;

    public Processor? Read(IConfigurationSection section)
    {
        if (section.GetTypeOf() != "mongo-event-types") return null;

        var processor = new MongoEventTypesProcessor
        (
            _obs.Context,
            section.ReadOptionalConfiguration<MongoEventTypesProcessor.Configuration>()
        );

        return processor.Process;
    }
}