using Konnect.Common;
using Konnect.Common.Extensions;
using Konnect.Hosting.Configuration;
using Konnect.Hosting.Dependencies;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Simple.Dotnet.Utilities.Buffers;

namespace Konnect.Host.Features.Actions.Grains;

public interface IActionSchedulerGrain : IGrainWithIntegerKey
{
    Task Schedule();
    Task Deactivate();
    Task<string[]> GetActions();
}

public sealed class ActionSchedulerGrain : Grain, IActionSchedulerGrain, IRemindable, IIncomingGrainCallFilter
{
    readonly IGrainFactory _grains;
    readonly ObservabilityContext _obs;
    readonly IConfigurationSection _configuration;
    readonly IOptionsMonitor<OrleansConfiguration> _orleansConfiguration;

    public ActionSchedulerGrain(
        IGrainFactory grains,
        Observability observability,
        IConfiguration configuration,
        IOptionsMonitor<OrleansConfiguration> orleansConfiguration)
    {
        _grains = grains;
        _orleansConfiguration = orleansConfiguration;
        _configuration = configuration.GetInstanceConfiguration();
        _obs = observability.Context.CreateNamed(nameof(ActionExecutionGrain));
    }

    public Task<string[]> GetActions() => Task.FromResult(_configuration.ReadActiveActions().ToArray());

    public async Task Schedule()
    {
        using var activeActions = _configuration.ReadActiveActions().AsRent(_configuration.GetActionSections().Count());
        _obs.ScheduleActions(activeActions.Written);
        await Task.WhenAll(activeActions.AsEnumerable().Select(s => _grains.GetGrain<IActionGrain>(s).PingExecutors()));
    }

    public async Task Deactivate()
    {
        using var activeActions = _configuration.ReadActiveActions().AsRent(_configuration.GetActionSections().Count());
        _obs.DeactivateActions(activeActions.Written);
        await Task.WhenAll(activeActions.AsEnumerable().Select(s => _grains.GetGrain<IActionGrain>(s).DeactivateExecutors()));
    }

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        _obs.OnActivation<ActionSchedulerGrain>();
        try
        {
            await this.RegisterOrUpdateReminder(_orleansConfiguration.CurrentValue.Actions.ReminderName, _orleansConfiguration.CurrentValue.Actions.ReminderPeriod, _orleansConfiguration.CurrentValue.Actions.ReminderPeriod);
            await base.OnActivateAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _obs.ActivationFailed<ActionSchedulerGrain>(ex);
        }
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        using var timer = _obs.OnDeactivation<ActionSchedulerGrain>(reason);
        return base.OnDeactivateAsync(reason, cancellationToken);
    }

    async Task IRemindable.ReceiveReminder(string reminderName, TickStatus status)
    {
        using (_obs.OnMethodExecution<ActionSchedulerGrain>(nameof(IRemindable.ReceiveReminder)))
        {
            try
            {
                await Schedule();
            }
            catch (Exception ex)
            {
                _obs.GrainExecutionFailed<ActionSchedulerGrain>(ex, nameof(IRemindable.ReceiveReminder));
            }
        }
    }

    async Task IIncomingGrainCallFilter.Invoke(IIncomingGrainCallContext context)
    {
        using (_obs.OnMethodExecution<ActionSchedulerGrain>(context.MethodName))
        {
            try
            {
                await context.Invoke();
            }
            catch (Exception ex)
            {
                _obs.GrainExecutionFailed<ActionSchedulerGrain>(ex, context.MethodName);
            }
        }
    }
}