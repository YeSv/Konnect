

using Konnect.Common.Extensions;
using Konnect.Hosting.Configuration;
using Konnect.Mongo;

using Orleans.Configuration;
using Orleans.Providers.MongoDB.Configuration;
using Bootstrapper = Microsoft.Extensions.Hosting.Host;

namespace Konnect.Host;

public class Program
{
    public static Task Main(string[] args) => CreateHostBuilder(args).RunConsoleAsync();

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Bootstrapper.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration(c => c.AddJsonFile("actions/actions.json", true, true).AddYamlFile("actions/actions.yaml", true, true))
            .UseOrleans((ctx, siloBuilder) =>
            {
                var section = ctx.Configuration.GetInstanceConfiguration().GetSection("orleans");
                var config = section.Read<OrleansConfiguration>(() => new());

                siloBuilder
                    .Configure<OrleansConfiguration>(section)
                    .Configure<ClusterMembershipOptions>(o => o
                        .Tap(o => o.DefunctSiloExpiration = config.Membership.DefunctExpiration)
                        .Tap(o => o.DefunctSiloCleanupPeriod = config.Membership.CleanupPeriod))
                    .Configure<SiloMessagingOptions>(o => o
                        .Tap(o => o.ResponseTimeout = config.Messaging.MessageTimeout)
                        .Tap(o => o.ResponseTimeoutWithDebugger = config.Messaging.MessageTimeout.Multiply(2)))
                    .Configure<ClusterOptions>(o => o
                        .Tap(o => o.ServiceId = nameof(global::Konnect.Common.Token.Konnect))
                        .Tap(o => o.ClusterId = ctx.Configuration.GetInstanceName()))
                    .Configure<GrainCollectionOptions>(o => o.CollectionAge = config.CollectionAge)
                    .UseMongoDBClient(s => 
                        s.GetRequiredService<Clusters>().Get(config.MongoDb.Type).Settings.Clone()
                            .Tap(c => c.ConnectTimeout = TimeSpan.FromSeconds(10)))
                    .UseMongoDBClustering(o => o
                        .Tap(o => o.Strategy = MongoDBMembershipStrategy.SingleDocument)
                        .Tap(o => o.DatabaseName = config.MongoDb.Database)
                        .Tap(o => o.CollectionPrefix = config.MongoDb.CollectionPrefix))
                    .UseMongoDBReminders(o => o
                        .Tap(o => o.DatabaseName = config.MongoDb.Database)
                        .Tap(o => o.CollectionPrefix = config.MongoDb.CollectionPrefix))
                    .UseDashboard(o => o
                        .Tap(o => o.BasePath = config.Dashboard.Path)
                        .Tap(o => o.Port = config.Dashboard.Port)
                        .Tap(o => o.HostSelf = true)
                        .Tap(o => o.CounterUpdateIntervalMs = (int)config.Dashboard.CounterUpdateInterval.TotalMilliseconds))
                    .AddMemoryGrainStorage("executionStore");

                if (ctx.Configuration.GetSection("ORLEANS_SERVICE_ID")?.Exists() is true) siloBuilder.UseKubernetesHosting();
            });
}