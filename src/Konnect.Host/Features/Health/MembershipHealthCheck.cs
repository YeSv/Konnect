using System.Threading.Tasks.Dataflow;
using Konnect.Common;
using Konnect.Hosting.Dependencies;
using Konnect.Host.Features.Health.Grains;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Konnect.Hosting;
using Orleans.Runtime;

namespace Konnect.Host.Features.Health;

public sealed class MembershipHealthCheck : IHealthCheck
{
    public static readonly string Name = "membership";

    volatile HealthResult _snapshot = new(Task.FromResult(HealthCheckResult.Healthy()), DateTime.MinValue);

    readonly ILocalSiloDetails _silo;
    readonly ObservabilityContext _obs;
    readonly IMembershipTable _membership;
    readonly ActionBlock<object?> _checker;
    readonly IOptionsMonitor<OrleansConfiguration> _configuration;
    
    public MembershipHealthCheck(ILocalSiloDetails silo, IMembershipTable membership, Observability observability, IOptionsMonitor<OrleansConfiguration> configuration)
    {
        _silo = silo;
        _membership = membership;
        _obs = observability.Context;
        _configuration = configuration;

        _checker = new(async _ =>
        {
            var health = await CheckMembership();
            _snapshot = new(Task.FromResult(health), DateTime.UtcNow);
        }, new() { MaxDegreeOfParallelism = 1, BoundedCapacity = 1 });
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var snapshot = _snapshot;

        // If snapshot is expired - ask for renewal
        if (snapshot.Time.Add(_configuration.CurrentValue.Health.MembershipCacheDuration) < DateTime.UtcNow) _checker.Post(null);

        return snapshot.Health;
    }

    async Task<HealthCheckResult> CheckMembership()
    {
        try
        {
            var data = await _membership.ReadAll();
            var currentSiloStatus = data?.Members.FirstOrDefault(f => f.Item1.SiloAddress?.Equals(_silo.SiloAddress) is true);

            var result = currentSiloStatus?.Item1?.Status switch
            {
                null => HealthCheckResult.Unhealthy("Current silo was not found in membership table"),
                SiloStatus.Active => HealthCheckResult.Healthy(),
                var s => HealthCheckResult.Unhealthy($"Current silo status is not Active but: {s}")
            };

            if (result.Status != HealthStatus.Healthy) _obs.MarkUnhealthyChecks(Name, result);

            return result;
        }
        catch (Exception ex)
        {
            var result = HealthCheckResult.Unhealthy("Something went wrong checking membership table", ex);
            _obs.MarkFaultyChecks(Name, result).MembershipCheckFailed(ex);
            return result;
        }
    }

    sealed record HealthResult(Task<HealthCheckResult> Health, DateTime Time);
}