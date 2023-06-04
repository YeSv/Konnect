using Konnect.Actions;
using Konnect.Common;
using Konnect.Hosting.Configuration;
using Konnect.Hosting.Dependencies;
using Konnect.Host.Features.Actions.States;
using Simple.Dotnet.Utilities.Buffers;
using Microsoft.Extensions.Options;

namespace Konnect.Host.Features.Actions.Grains;

public interface IActionGrain : IGrainWithStringKey
{
    Task PingExecutors();

    Task StartExecutors();
    Task StopExecutors();
    
    Task DeactivateExecutors();
    
    Task<ExecutionStatus[]> GetStatuses();
}

public sealed class ActionGrain : Grain, IActionGrain, IIncomingGrainCallFilter
{
    readonly ObservabilityContext _obs;
    readonly IConfigurationSection _instance;
    readonly IOptionsMonitor<OrleansConfiguration> _orleans;

    public ActionGrain(Observability observability, IOptionsMonitor<OrleansConfiguration> orleans, IConfiguration configuration) 
    {
        _orleans = orleans;
        _instance = configuration.GetInstanceConfiguration();
        _obs = observability.Context.CreateNamed(this.GetPrimaryKeyString());
    }

    public Task PingExecutors() => CallExecutors(g => g.Ping(), nameof(IActionExecutionGrain.Ping), true);

    public Task StartExecutors() => CallExecutors(g => g.Start(), nameof(IActionExecutionGrain.Start));

    public Task StopExecutors() => CallExecutors(g => g.Stop(), nameof(IActionExecutionGrain.Stop));

    public Task DeactivateExecutors() => CallExecutors(g => g.Deactivate(), nameof(IActionExecutionGrain.Deactivate), true);

    public Task<ExecutionStatus[]> GetStatuses() => CallExecutors(g => g.GetStatus(), nameof(IActionExecutionGrain.GetStatus));

    public override Task OnActivateAsync(CancellationToken token)
    {
        _obs.OnActivation<ActionGrain>();
        return base.OnActivateAsync(token);
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken token)
    {   
        _obs.OnActivation<ActionGrain>();
        return base.OnDeactivateAsync(reason, token);
    }

    Task TriggerAction()
    {
        return Task.CompletedTask;
    }

    async Task CallExecutors(Func<IActionExecutionGrain, Task> executor, string method, bool useSkew = false) 
    {
        var action = this.GetPrimaryKeyString();
        var config = _instance.GetActionSection(action)?.ReadConfiguration<ActionConfiguration>();
        
        if (config is null) return;

        if (useSkew)
        {
            await Task.Delay(Random.Shared.Next(
                (int)TimeSpan.FromSeconds(1).TotalMilliseconds, 
                (int)_orleans.CurrentValue.Actions.BroadcastSkew.TotalMilliseconds));
        }

        using var calls = new Rent<Task>(config.Replicas);
        for (var i = 0; i < config.Replicas; i++) 
            calls.Append(executor(GrainFactory.GetGrain<IActionExecutionGrain>(i, action, null)));

        await Task.WhenAll(calls.AsEnumerable());
    }

    async Task<T[]> CallExecutors<T>(Func<IActionExecutionGrain, Task<T>> executor, string method, bool useSkew = false) 
    {
        var action = this.GetPrimaryKeyString();
        var config = _instance.GetActionSection(action)?.ReadConfiguration<ActionConfiguration>();

        if (config is null) return Array.Empty<T>();

        if (useSkew)
        {
            await Task.Delay(Random.Shared.Next(
                (int)TimeSpan.FromSeconds(1).TotalMilliseconds, 
                (int)_orleans.CurrentValue.Actions.BroadcastSkew.TotalMilliseconds));
        }

        using var calls = new Rent<Task<T>>(config.Replicas);
        for (var i = 0; i < config.Replicas; i++)
            calls.Append(executor(GrainFactory.GetGrain<IActionExecutionGrain>(i, action, null)));

        return await Task.WhenAll(calls.AsEnumerable());
    }

    async Task IIncomingGrainCallFilter.Invoke(IIncomingGrainCallContext context)
    {
        try
        {
            using (_obs.OnMethodExecution<ActionGrain>(context.MethodName))
            {
                await context.Invoke();
            }
        }
        catch (Exception ex)
        {
            _obs.GrainExecutionFailed<ActionGrain>(ex, context.MethodName);
            throw;
        }
    }
}