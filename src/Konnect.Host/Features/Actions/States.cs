
namespace Konnect.Host.Features.Actions.States;

public enum ExecutionStatus : byte { Idle, Running, Stopped }

[GenerateSerializer]
public sealed class ExecutionState
{
    [Id(0)] public ExecutionStatus Status { get; set; } = ExecutionStatus.Idle;
    [Id(1)] public DateTime StateChangeTime { get; set; } = DateTime.UtcNow;

    public ExecutionState TransitionTo(ExecutionStatus status)
    {
        Status = status;
        StateChangeTime = DateTime.UtcNow;
        return this;
    }
}



public sealed class ActionDescriptor : IAsyncDisposable
{
    public ActionDescriptor(Func<CancellationToken, Task> actionFn) 
    {
        Switch = new();
        Runner = actionFn(Switch.Token);
    }

    public Task Runner { get; }   
    public CancellationTokenSource Switch { get; }

    public ValueTask DisposeAsync() 
    {
        Switch.Cancel();
        return Runner switch
        {
            { IsCompleted: true } => new(),
            _ => new(Runner)
        };
    }
}