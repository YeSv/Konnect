
using System.Threading.Tasks.Dataflow;
using Konnect.Common;
using Konnect.Hosting.Dependencies;
using Konnect.Host.Features.Health.Grains;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Konnect.Hosting;

namespace Konnect.Host.Features.Health;

public sealed class SiloHealthCheck : IHealthCheck
{
    public static readonly string Name = "silo";

    volatile HealthResult _snapshot = new(Task.FromResult(HealthCheckResult.Healthy()), DateTime.MinValue);

    readonly IGrainFactory _grains;
    readonly ObservabilityContext _obs;
    readonly ActionBlock<object?> _checker;
    readonly IOptionsMonitor<OrleansConfiguration> _configuration;
    
    public SiloHealthCheck(IGrainFactory grains, Observability observability, IOptionsMonitor<OrleansConfiguration> configuration)
    {
        _grains = grains;
        _obs = observability.Context;
        _configuration = configuration;

        _checker = new(async _ =>
        {
            var health = await CheckSilo();
            _snapshot = new(Task.FromResult(health), DateTime.UtcNow);
        }, new() { MaxDegreeOfParallelism = 1, BoundedCapacity = 1 });
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var snapshot = _snapshot;

        // If snapshot is expired - ask for renewal
        if (snapshot.Time.Add(_configuration.CurrentValue.Health.SiloCacheDuration) < DateTime.UtcNow) _checker.Post(null);

        return snapshot.Health;
    }

    async Task<HealthCheckResult> CheckSilo()
    {
        try
        {
            await _grains.GetGrain<IHealthGrain>(default(int)).Ping();
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            var result = HealthCheckResult.Unhealthy("Something went wrong checking silo", ex);
            _obs.MarkFaultyChecks(Name, result).SiloCheckFailed(ex);
            return result;
        }
    }

    sealed record HealthResult(Task<HealthCheckResult> Health, DateTime Time);
}