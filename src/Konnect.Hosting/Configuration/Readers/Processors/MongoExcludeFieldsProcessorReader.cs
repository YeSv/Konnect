using Konnect.Hosting.Dependencies;
using Konnect.Mongo;
using Konnect.Processing.Common.Processors;
using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration.Readers.Processors;

public sealed class MongoExcludeFieldsProcessorReader : IConfigurationReader<Processor>
{
    readonly Observability _obs;

    public MongoExcludeFieldsProcessorReader(Observability obs) => _obs = obs;


    public Processor? Read(IConfigurationSection section) 
    {
        if (section.GetTypeOf() != "mongo-exclude-fields") return null;

        var configuration = section.ReadConfiguration<MongoExcludeFieldsProcessor.Configuration>();
        var processor = new MongoExcludeFieldsProcessor(_obs.Context, configuration);

        return processor.Process;
    }
}