using Konnect.Actions;
using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration.Readers.Actions;

public sealed class ActionReader : IConfigurationReader<ActionFn>
{
    readonly IConfigurationReader<PipelineFn>[] _readers;

    public ActionReader(IEnumerable<IConfigurationReader<PipelineFn>> readers) => _readers = readers.ToArray();

    public ActionFn Read(IConfigurationSection section)
    {
        var config = section.ReadConfiguration<ActionConfiguration>();
        var pipelineFn = section.ReadPipeline(_readers);
         
        return (ctx, token) => Konnect.Actions.Action.Run(new(pipelineFn!), new(ctx, config, token));
    }
}