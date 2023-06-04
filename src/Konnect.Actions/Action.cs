using Konnect.Common;
using Konnect.Common.Extensions;

namespace Konnect.Actions;

public delegate Task ActionExecutor(ObservabilityContext context, CancellationToken token);
public readonly record struct ActionContext(ObservabilityContext Observability, ActionConfiguration Configuration, CancellationToken Token);

public sealed class ActionConfiguration
{
    public bool Active { get; set; } = true;
    public string Name { get; set; } = string.Empty;
    public int Retries { get; set; } = int.MaxValue;
    public int Replicas { get; set; } = 1;
    public TimeSpan RetryPause { get; set; } = TimeSpan.FromMinutes(1);
}

public static class Action
{
    public static Task Run(ActionExecutor executor, ActionContext context) => Task.Run(async () =>
    {
        var (obs, configuration, token) = context;
        var (name, retries, pause) = (configuration.Name, configuration.Retries, configuration.RetryPause);

        try 
        {
            while (true)
            {
                token.ThrowIfCancellationRequested();
                try
                {
                    await executor(obs.Started(), token);
                    return;
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !token.IsCancellationRequested)
                {
                    obs.Error(ex);
                    if (retries-- > 0) await Task.Delay(pause.Tap(obs, (p, c) => c.PauseOnError(p, retries)), token);
                    else 
                    {
                        obs.Fault();
                        return;
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            obs.Stopped();
        }
    }, context.Token);
}