
using System.Runtime.CompilerServices;
using App.Metrics;
using App.Metrics.Counter;
using App.Metrics.Timer;
using Konnect.Common;
using Konnect.Host.Features.Actions.Grains;
using Konnect.Host.Features.Actions.Tasks;

namespace Konnect.Host.Features;

public static class OrleansMetrics
{
    public static readonly string Context = $"{RootMetrics.Context}.Orleans";

    public static readonly CounterOptions GrainErrors = new()
    {
        Context = Context,
        ResetOnReporting = true,
        MeasurementUnit = Unit.Errors,
        Name = nameof(GrainErrors),
    };

    public static readonly TimerOptions GrainMethodDuration = new()
    {
        Context = Context,
        ResetOnReporting = true,
        MeasurementUnit = Unit.Items,
        DurationUnit = TimeUnit.Milliseconds,
        Name = nameof(GrainMethodDuration),
    };

    public static readonly TimerOptions GrainDeactivationDuration = new()
    {
        Context = Context,
        ResetOnReporting = true,
        MeasurementUnit = Unit.Items,
        DurationUnit = TimeUnit.Milliseconds,
        Name = nameof(GrainDeactivationDuration),
    };

    public static readonly TimerOptions GrainCallDuration = new()
    {
        Context = Context,
        ResetOnReporting = true,
        MeasurementUnit = Unit.Items,
        DurationUnit = TimeUnit.Milliseconds,
        Name = nameof(GrainCallDuration),
    };
}

public static class OrleansObservability
{
    public static void GrainExecutionFailed<T>(this ObservabilityContext ctx, Exception ex, [CallerMemberName] string action = "")
    {
        ctx.Logger.CreateLogger<T>().LogError(ex, $"""Failed to perform method "{action}" on a grain "{ctx.GetName()}" """);
        ctx.Metrics.Measure.Counter.Increment(OrleansMetrics.GrainErrors, new(RootMetrics.Tags.ContextName, ctx.GetName()), 1);
    }

    public static void DeactivationFailed<T>(this ObservabilityContext ctx, Exception ex)
    {
        ctx.Logger.CreateLogger<T>().LogError(ex, $"Failed to deactivate a grain. Context: {ctx.GetName()}");
        ctx.Metrics.Measure.Counter.Increment(OrleansMetrics.GrainErrors, new(RootMetrics.Tags.ContextName, ctx.GetName()), 1);
    }

    public static void ActivationFailed<T>(this ObservabilityContext ctx, Exception ex)
    {
        ctx.Logger.CreateLogger<T>().LogError(ex, $"Failed to activate a grain. Context: {ctx.GetName()}");
        ctx.Metrics.Measure.Counter.Increment(OrleansMetrics.GrainErrors, new(RootMetrics.Tags.ContextName, ctx.GetName()), 1);
    }

    public static IDisposable OnDeactivation<T>(this ObservabilityContext ctx, DeactivationReason reason)
    {
        ctx.Logger.CreateLogger<T>().LogInformation(reason.Exception, $"Deactivated action grain {ctx.GetName()}. Reason[{reason.ReasonCode}]: {reason.Description}");
        return ctx.Metrics.Measure.Timer.Time(OrleansMetrics.GrainDeactivationDuration, new MetricTags(RootMetrics.Tags.ContextName, ctx.GetName()));
    }

    public static IDisposable OnMethodExecution<T>(this ObservabilityContext ctx, string method) =>
        ctx.Metrics.Measure.Timer.Time(OrleansMetrics.GrainMethodDuration, new MetricTags(new[] { RootMetrics.Tags.ContextName, RootMetrics.Tags.MethodName }, new[] { ctx.GetName(), method }));

    public static IDisposable MeasureGrainCallDuration(this ObservabilityContext ctx, string method) =>
        ctx.Metrics.Measure.Timer.Time(OrleansMetrics.GrainCallDuration, new MetricTags(RootMetrics.Tags.MethodName, method));


    public static void OnActivation<T>(this ObservabilityContext ctx) =>
        ctx.Logger.CreateLogger<T>().LogInformation($"Activated a grain. Context: {ctx.GetName()}");

    public static void BootstrapRetryFailed(this ObservabilityContext ctx, Exception ex, TimeSpan nextRetry) =>
        ctx.Logger.CreateLogger<ActionsBootstrapTask>().LogWarning(ex, $"Failed to bootstrap actions. Will retry in {nextRetry}");

    public static void BootstrapFailed(this ObservabilityContext ctx, Exception ex) =>
        ctx.Logger.CreateLogger<ActionsBootstrapTask>().LogError(ex, "Failed to bootstrap actions");

    
    public static void BootstrapStarted(this ObservabilityContext ctx) =>
        ctx.Logger.CreateLogger<ActionsBootstrapTask>().LogInformation("Started to bootstrap actions");

    
    public static void BootstrapFinished(this ObservabilityContext ctx) =>
        ctx.Logger.CreateLogger<ActionsBootstrapTask>().LogInformation("Finshed to bootstrap actions");

    public static void ScheduleActions(this ObservabilityContext ctx, int total) =>
        ctx.Logger.CreateLogger<ActionSchedulerGrain>().LogInformation($"Scheduling total of {total} actions");

    public static void DeactivateActions(this ObservabilityContext ctx, int total) =>
        ctx.Logger.CreateLogger<ActionSchedulerGrain>().LogInformation($"Deactivating total of {total} actions");
}