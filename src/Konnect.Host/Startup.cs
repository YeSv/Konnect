using Konnect.Hosting;
using Konnect.Host.Features.Actions.Tasks;
using Konnect.Host.Features.Health;
using Orleans.Runtime;
using System.Text.Json.Serialization;
using Konnect.Hosting.Health;

namespace Konnect.Host;

public sealed class Startup
{
    readonly IConfigurationRoot _configurationRoot;

    public Startup(IConfiguration config) => _configurationRoot = (IConfigurationRoot)config;

    public void ConfigureServices(IServiceCollection services)
    {
        services
            .AddControllers()
            .AddJsonOptions(s => s.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter()))
            .Services
            .AddKonnectDependencies(_configurationRoot)
            .AddTransient<ILifecycleParticipant<ISiloLifecycle>, ActionsBootstrapTask>()
            .AddSingleton<SiloHealthCheck>()
            .AddSingleton<MembershipHealthCheck>()
            .AddSingleton<KonnectHealth>()
            .AddHealthChecks()
            .AddCheck<KonnectHealth>(KonnectHealth.Name)
            .AddCheck<SiloHealthCheck>(SiloHealthCheck.Name)
            .AddCheck<MembershipHealthCheck>(MembershipHealthCheck.Name);
    }

    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        app.UseRouting();
        app.UseOrleansDashboard();
        app.UseEndpoints(e => e.MapControllers());
    }
}


