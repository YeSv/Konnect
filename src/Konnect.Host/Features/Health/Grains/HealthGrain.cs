using Orleans.Concurrency;

namespace Konnect.Host.Features.Health.Grains;

public interface IHealthGrain : IGrainWithIntegerKey
{
    Task Ping();
}

[StatelessWorker(1)]
public sealed class HealthGrain : IHealthGrain
{
    public Task Ping() => Task.CompletedTask;
}