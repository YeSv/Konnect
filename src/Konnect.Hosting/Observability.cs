using App.Metrics;
using App.Metrics.Counter;
using Konnect.Common;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace Konnect.Hosting;

public static class HostingMetrics
{
    public static readonly string Context = $"{RootMetrics.Context}.Hosting";

    public static readonly CounterOptions UnhealthyChecks = new()
    {
        Context = Context,
        ResetOnReporting = true,
        MeasurementUnit = Unit.Errors,
        Name = nameof(UnhealthyChecks),
    };
}

public static class HostingObservability
{
    public static ObservabilityContext MarkUnhealthyChecks(this ObservabilityContext obs, string name, HealthCheckResult result)
    {
        obs.Logger.CreateLogger<Common.Token.Konnect>().LogWarning(result.Exception, $"""Health check "{name}" returned unhealthy status. Message: "{result.Description}" """);
        obs.Metrics.Measure.Counter.Increment(HostingMetrics.UnhealthyChecks, new(RootMetrics.Tags.ContextName, name), 1);
        return obs;
    }

    public static ObservabilityContext MarkFaultyChecks(this ObservabilityContext obs, string name, HealthCheckResult result)
    {
        obs.Logger.CreateLogger<Common.Token.Konnect>().LogError(result.Exception, $"""Health check "{name}" returned unhealthy status. Service may stop. Message: "{result.Description}" """);
        obs.Metrics.Measure.Counter.Increment(HostingMetrics.UnhealthyChecks, new(RootMetrics.Tags.ContextName, name), 1);
        return obs;
    }
}