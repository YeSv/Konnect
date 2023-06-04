using Konnect.Common;
using Konnect.Common.Extensions;
using Konnect.Hosting.Configuration;
using Konnect.Hosting.Configuration.Readers;
using Konnect.Hosting.Dependencies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using System.Threading.Tasks.Dataflow;

namespace Konnect.Hosting.Health;

public sealed class HealthConfiguration
{
    public TimeSpan CacheDuration { get; set; } = TimeSpan.FromSeconds(5);
}

public sealed class KonnectHealth : IHealthCheck
{
    public static readonly string Name = "konnect";

    volatile HealthResult _snapshot = new(Task.FromResult(HealthCheckResult.Healthy()), DateTime.MinValue);

    readonly CheckFn[] _checks;
    readonly ObservabilityContext _obs;
    readonly HealthConfiguration _config;
    readonly ActionBlock<object?> _checker;

    public KonnectHealth(
        Observability observability,
        IConfiguration configuration,
        IEnumerable<IConfigurationReader<CheckFn>> readers)
    {
        _obs = observability.Context;
        _checks = configuration.GetInstanceConfiguration().GetHealthSection().ReadChecks(readers);
        _config = configuration.GetInstanceConfiguration().GetHealthSection().ReadOptionalConfiguration<HealthConfiguration>();
        
        _checker = new(async _ =>
        {
            var health = await CheckHealth();
            _snapshot = new(Task.FromResult(health), DateTime.UtcNow);
        }, new() { MaxDegreeOfParallelism = 1, BoundedCapacity = 1 });
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken token = default)
    {
        var snapshot = _snapshot;

        // If snapshot is expired - ask for renewal
        if (snapshot.Time.Add(_config.CacheDuration) < DateTime.UtcNow) _checker.Post(null);

        return snapshot.Health;
    }

    async Task<HealthCheckResult> CheckHealth()
    {
        try
        {
            using var cts = new CancellationTokenSource(_config.CacheDuration);

            var results = await Task.WhenAll(_checks.Select(c => c(cts.Token)));
            if (results.All(r => r.CheckResult.Status == HealthStatus.Healthy)) return new(HealthStatus.Healthy);

            var (status, details) = (HealthStatus.Healthy, new Dictionary<string, object?>());
            foreach (var result in results)
            {
                if (result.CheckResult.Status == HealthStatus.Healthy) continue;
                _obs.MarkUnhealthyChecks(result.Name, result.CheckResult);
            }

            return new(status, "Some healthchecks have unhealthy statuses", data: details!);
        }
        catch (Exception ex)
        {
            var result = new HealthCheckResult(HealthStatus.Healthy, "An error occurred", ex);
            _obs.MarkUnhealthyChecks(Name, result);
            return result;
        }
    }


    sealed record HealthResult(Task<HealthCheckResult> Health, DateTime Time);
}