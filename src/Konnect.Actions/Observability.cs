using App.Metrics.Counter;
using Konnect.Common;
using Konnect.Common.Extensions;
using Microsoft.Extensions.Logging;
using Unit = App.Metrics.Unit;

namespace Konnect.Actions;

public static class ActionObservability
{
    readonly record struct ActionContext();

    static readonly string Context = $"{RootMetrics.Context}.Actions";

    static readonly CounterOptions ExecutionsCount = new CounterOptions
    {
        Context = Context,
        ResetOnReporting = true,
        MeasurementUnit = Unit.Calls,
        Name = nameof(ExecutionsCount),
    }; 

    static readonly CounterOptions ErrorsCount = new CounterOptions
    {
        Context = Context,
        ResetOnReporting = true,
        Name = nameof(ErrorsCount),
        MeasurementUnit = Unit.Errors,
    };

    static readonly CounterOptions FaultsCount = new CounterOptions
    {
        Context = Context,
        ResetOnReporting = true,
        Name = nameof(FaultsCount),
        MeasurementUnit = Unit.Errors,
    };

    public static class Tags
    {
        public static readonly string Execution = nameof(Execution);

        public static readonly string[] ContextNameAndExecution = new[] { RootMetrics.Tags.ContextName, Execution };
    }

    public static string? GetExecution(this ObservabilityContext o) 
    {
        if (o.Attributes?.TryGetValue(Tags.Execution, out var action) is not true) return o.GetName();
        return action as string ?? o.GetName();
    }

    public static ObservabilityContext WithExecution(this ObservabilityContext o, string name) => o.WithAttribute(Tags.Execution, name);

    public static ObservabilityContext Started(this ObservabilityContext o) => o.Tap(o => 
    {
        o.Logger.CreateLogger<ActionContext>().LogInformation($"Started action {o.GetName()}[{o.GetExecution()}]");
        o.Metrics.Measure.Counter.Increment(ExecutionsCount, new(Tags.ContextNameAndExecution, new[] { o.GetName(), o.GetExecution() }), 1);
    });

    public static ObservabilityContext Error(this ObservabilityContext o, Exception ex) => o.Tap(o =>
    {
        o.Logger.CreateLogger<ActionContext>().LogWarning(ex, $"Unhandled error occurred during action execution of {o.GetName()}[{o.GetExecution()}]");
        o.Metrics.Measure.Counter.Increment(ErrorsCount, new(Tags.ContextNameAndExecution, new[] { o.GetName(), o.GetExecution() }), 1);
    });

    public static ObservabilityContext Fault(this ObservabilityContext o)
    {
        o.Logger.CreateLogger<ActionContext>().LogError($"Action {o.GetName()}[{o.GetExecution()}] is stopped due to num of retries reached");
        o.Metrics.Measure.Counter.Increment(FaultsCount, new(Tags.ContextNameAndExecution, new[] { o.GetName(), o.GetExecution() }), 1);

        return o;
    }

    public static ObservabilityContext Stopped(this ObservabilityContext o) => o.Tap(o =>
        o.Logger.CreateLogger<ActionContext>().LogInformation($"Action {o.GetName()}[{o.GetExecution()}] is stopped"));

    public static ObservabilityContext PauseOnError(this ObservabilityContext o, TimeSpan pause, int retries) => o.Tap(o =>
        o.Logger.CreateLogger<ActionContext>().LogInformation($"Action {o.GetName()}[{o.GetExecution()}] is paused for {pause}. Retries left: {retries}"));
}