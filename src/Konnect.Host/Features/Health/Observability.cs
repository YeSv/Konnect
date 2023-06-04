using Konnect.Common;

namespace Konnect.Host.Features.Health;

public static class HealthObservability
{
    public static void MembershipCheckFailed(this ObservabilityContext ctx, Exception ex) =>
        ctx.Logger.CreateLogger<MembershipHealthCheck>().LogWarning(ex, "Something went wrong checking membership table. Silo seems like disconnected");

    public static void SiloCheckFailed(this ObservabilityContext ctx, Exception ex) =>
        ctx.Logger.CreateLogger<SiloHealthCheck>().LogWarning(ex, "Something went wrong checking silo health. It seems unresponsive");
}