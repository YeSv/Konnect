using Konnect.Common;
using Konnect.Common.Health;
using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration.Readers;

public interface IConfigurationReader<T>
{
    T? Read(IConfigurationSection section);
}

public delegate Task<Result> CheckFn(CancellationToken token);
public delegate Task ActionFn(ObservabilityContext context, CancellationToken token);
public delegate Task PipelineFn(ObservabilityContext context, CancellationToken token);