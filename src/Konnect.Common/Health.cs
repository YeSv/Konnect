using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Konnect.Common.Health;

public readonly record struct Result(string Name, bool Ignore, HealthCheckResult CheckResult);