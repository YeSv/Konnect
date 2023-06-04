using App.Metrics;
using Microsoft.Extensions.Logging;

namespace Konnect.Common;

public readonly record struct ObservabilityContext(IMetrics Metrics, ILoggerFactory Logger, Dictionary<string, object>? Attributes = null);

public static class RootMetrics
{
    public static readonly string Context = nameof(Token.Konnect);

    public static class Tags 
    {
        public static readonly string ContextName = nameof(ContextName);
        public static readonly string MethodName = nameof(MethodName);
    }
}

public static class ObservabilityExtensions
{
    public static string GetName(this ObservabilityContext ctx) 
    {
        if (ctx.Attributes?.TryGetValue(RootMetrics.Tags.ContextName, out var context) is not true) return "Unknown";
        return context as string ?? "Unknown";
    }

    public static ObservabilityContext WithAttribute(this ObservabilityContext ctx, string name, object value)
    {
        var attributes = ctx.Attributes ?? new();
        attributes[name] = value;
        return new
        (
            ctx.Metrics, 
            ctx.Logger, 
            attributes
        );
    }

    public static ObservabilityContext CreateNamed(this ObservabilityContext ctx, string name) =>
        new ObservabilityContext
        (
            ctx.Metrics, 
            ctx.Logger, 
            ctx.Attributes?.ToDictionary(a => a.Key, a => a.Value)
        ).WithAttribute(RootMetrics.Tags.ContextName, name);
}