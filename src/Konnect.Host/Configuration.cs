namespace Konnect.Host;

public sealed class OrleansConfiguration
{
    public TimeSpan CollectionAge { get; set; } = TimeSpan.FromMinutes(5);
    
    public HealthCheckConfiguration Health { get; set; } = new();
    public MessagingConfiguration Messaging { get; set; } = new();
    public DashboardConfiguration Dashboard { get; set; } = new();
    public MembershipConfiguration Membership { get; set; } = new();
    public ActionsFeatureConfiguration Actions { get; set; } = new();
    public MongoDbMembershipConfiguration MongoDb { get; set; } = new();
}

public sealed class MessagingConfiguration
{
    public TimeSpan MessageTimeout { get; set; } = TimeSpan.FromSeconds(20);
}

public sealed class HealthCheckConfiguration
{
    public TimeSpan SiloCacheDuration { get; set; } = TimeSpan.FromSeconds(20);
    public TimeSpan MembershipCacheDuration { get; set; } = TimeSpan.FromSeconds(20); // Should be >= MessageTimeout
}

public sealed class MongoDbMembershipConfiguration
{
    public string Type { get; set; } = string.Empty;
    public string Database { get; set; } = string.Empty;
    public string CollectionPrefix { get; set; } = "Konnect";
}

public sealed class DashboardConfiguration
{
    public int Port { get; set; } = 9999;
    public string Path { get; set; } = "/dashboard";
    public TimeSpan CounterUpdateInterval { get; set; } = TimeSpan.FromSeconds(10);
}

public sealed class ActionsFeatureConfiguration
{
    public string ReminderName { get; set; } = "actions";
    
    public int BootstrapRetries { get; set; } = 10;
    public TimeSpan BootstrapFailPause { get; set; } = TimeSpan.FromSeconds(10);

    public TimeSpan ReminderPeriod { get; set; } = TimeSpan.FromMinutes(3);
    public TimeSpan BroadcastSkew { get; set; } = TimeSpan.FromSeconds(5);
}

public sealed class MembershipConfiguration
{
    public TimeSpan CleanupPeriod { get; set; } = TimeSpan.FromMinutes(10);
    public TimeSpan DefunctExpiration { get; set; } = TimeSpan.FromMinutes(10);
}