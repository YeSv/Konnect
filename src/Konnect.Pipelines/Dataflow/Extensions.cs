using System.Threading.Tasks.Dataflow;

namespace Konnect.Pipelines.Dataflow;

public static class DataflowExtensions
{
    public static ValueTask Send<T>(this ITargetBlock<T> target, T data, TimeSpan? pause = default, CancellationToken token = default)
    {
        return target.Post(data) switch
        {
            true => new(),
            false => new(SendAsync(target, data, pause ?? TimeSpan.FromMilliseconds(250), token))
        };

        static async Task SendAsync(ITargetBlock<T> target, T data, TimeSpan pause, CancellationToken token)
        {
            while (!token.IsCancellationRequested) 
            {
                if (target.Post(data)) return;

                await Task.Delay(pause, token);
            }
        }
    }

    public static async Task CompletePipelineWithTimeout<TContext>(this Task completion, TimeSpan timeout, TContext ctx, Action<TContext> onTimeout)
    {
        var timeoutTask = Task.Delay(timeout);

        var completedTask = await Task.WhenAny(timeoutTask, completion);
        if (completedTask == timeoutTask) onTimeout(ctx);

        await completedTask;
    }

    public static async Task WaitForPipelineCompletion<TContext>(this Task completion, TContext ctx, Action<TContext, Exception> onError)
    {
        try
        {
            await completion;
        }
        catch (OperationCanceledException) {}
        catch (Exception ex)
        {
            onError(ctx, ex);
        }
    }
}