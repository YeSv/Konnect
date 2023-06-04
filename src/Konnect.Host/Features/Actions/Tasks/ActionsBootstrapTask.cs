using Konnect.Common;
using Konnect.Hosting.Dependencies;
using Konnect.Host.Features.Actions.Grains;
using Orleans.Runtime;
using Polly;
using Microsoft.Extensions.Options;

namespace Konnect.Host.Features.Actions.Tasks;

public sealed class ActionsBootstrapTask : ILifecycleParticipant<ISiloLifecycle>
{
    readonly AsyncPolicy _policy;
    readonly IGrainFactory _grains;
    readonly ObservabilityContext _obs;
    readonly IOptionsMonitor<OrleansConfiguration> _configuraiton;

    public ActionsBootstrapTask(
        Observability observability, 
        IGrainFactory grains,
        IOptionsMonitor<OrleansConfiguration> configuration) 
    {
        _grains = grains;
        _obs = observability.Context;
        _configuraiton = configuration;
        _policy = Policy.Handle<Exception>().WaitAndRetryAsync(
            _configuraiton.CurrentValue.Actions.BootstrapRetries,
            r => _configuraiton.CurrentValue.Actions.BootstrapFailPause,
            (ex, t) => _obs.BootstrapRetryFailed(ex, t));
    }

    public void Participate(ISiloLifecycle observer) => observer
        .Subscribe<ActionsBootstrapTask>(ServiceLifecycleStage.Active, OnActivation);

    async Task OnActivation(CancellationToken cancellationToken)
    {
        try
        {
            await _policy.ExecuteAsync(async t =>
            {
                _obs.BootstrapStarted();
                await _grains.GetGrain<IActionSchedulerGrain>(default(int)).Deactivate();
                await _grains.GetGrain<IActionSchedulerGrain>(default(int)).Schedule();
                _obs.BootstrapFinished();
            }, cancellationToken);
        }
        catch (Exception ex)
        {
            _obs.BootstrapFailed(ex);
        }
    }
}