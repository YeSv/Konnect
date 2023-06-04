using Konnect.Common;
using Konnect.Common.Extensions;
using Konnect.Hosting.Configuration;
using Konnect.Hosting.Configuration.Readers;
using Konnect.Hosting.Dependencies;
using Konnect.Host.Features.Actions.States;
using Orleans.Runtime;
using Konnect.Actions;

namespace Konnect.Host.Features.Actions.Grains;

public interface IActionExecutionGrain : IGrainWithIntegerCompoundKey
{
    Task Ping();
    Task Stop();
    Task Start();
    Task Deactivate();
    Task<ExecutionStatus> GetStatus();
}

public sealed class ActionExecutionGrain : Grain, IActionExecutionGrain, IIncomingGrainCallFilter
{
    ActionDescriptor? _action;

    readonly Storages _storages;
    readonly ObservabilityContext _obs;
    readonly IConfigurationSection _instance;
    readonly IPersistentState<ExecutionState> _state;
    readonly IConfigurationReader<ActionFn> _actionReader;

    public ActionExecutionGrain(
        Storages storages,
        Observability observability,
        IConfiguration configuration,
        [PersistentState("state", "executionStore")] IPersistentState<ExecutionState> state,
        IConfigurationReader<ActionFn> actionReader)
    {
        _state = state;
        _storages = storages;
        _actionReader = actionReader;
        _instance = configuration.GetInstanceConfiguration();
        _obs = observability.Context.CreateNamed($"{this.GetPrimaryKeyLong(out var ext)}-{ext}");
    }
    
    public Task Ping() => (_state.State.Status, _action?.Runner) switch 
    {
        (ExecutionStatus.Stopped, _) => Task.CompletedTask,
        (_, _) => Start()
    };

    public async Task Start()
    {
        if (_action is { Runner.IsCompleted: false }) return;
        
        _action = new(RunAction);
        await _state.Tap(s => s.State.TransitionTo(ExecutionStatus.Running)).WriteStateAsync();
    }

    public async Task Stop()
    {
        if (_action != null) await _action.DisposeAsync();
        await _state.Tap(s => s.State.TransitionTo(ExecutionStatus.Stopped)).WriteStateAsync();
    }

    public Task Deactivate()
    {
        DeactivateOnIdle();
        return Task.CompletedTask;
    }

    public Task<ExecutionStatus> GetStatus() => Task.FromResult(_state.State.Status);

    public override Task OnActivateAsync(CancellationToken token)
    {
        _obs.OnActivation<ActionExecutionGrain>();
        return base.OnActivateAsync(token);
    }

    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken token)
    {   
        using var timer = _obs.OnDeactivation<ActionExecutionGrain>(reason);
        try
        {
            if (_action != null) await _action.DisposeAsync();
        
            await _state.ClearStateAsync();
    
            await base.OnDeactivateAsync(reason, token);
        }
        catch (Exception ex)
        {
            _obs.DeactivationFailed<ActionExecutionGrain>(ex);
            throw;
        }
    }

    async Task IIncomingGrainCallFilter.Invoke(IIncomingGrainCallContext context)
    {
        try
        {
            using (_obs.OnMethodExecution<ActionExecutionGrain>(context.MethodName))
            {
                await context.Invoke();
            }
        }
        catch (Exception ex)
        {
            _obs.GrainExecutionFailed<ActionExecutionGrain>(ex, context.MethodName);
            throw;
        }
    }

    Task RunAction(CancellationToken token)
    {
        var replicaId = this.GetPrimaryKeyLong(out var action);
        var observability = new ObservabilityContext(_obs.Metrics, _obs.Logger).CreateNamed(action).WithExecution(_obs.GetName());
        return _actionReader.Read(_instance.GetActionSection(action)!)!(observability, token);
    }
}