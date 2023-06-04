using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;

namespace Konnect.Pipelines.Dataflow.Blocks;

public sealed record LazyBatchBlockOptions(
    int BatchSize,
    TimeSpan? OfferPause = null,
    ExecutionDataflowBlockOptions? TargetOptions = null,
    ExecutionDataflowBlockOptions? SourceOptions = null,
    BoundedChannelOptions? ChannelOptions = null,
    bool CompleteOnError = true);

// "Lazy" batch block that does not wait for the whole batch of expected size to be provided
public static class LazyBatchBlock
{
    public static IPropagatorBlock<TFrom, TTo[]> Create<TFrom, TTo>(
        Func<TFrom, TTo> mapper,
        Action<Exception> onError,
        LazyBatchBlockOptions options,
        CancellationToken token = default)
    {
        var targetOpts = options.TargetOptions ?? new() { MaxDegreeOfParallelism = 1, BoundedCapacity = DataflowBlockOptions.Unbounded };
        var sourceOpts = options.SourceOptions ?? new() { MaxDegreeOfParallelism = 1, BoundedCapacity = DataflowBlockOptions.Unbounded };
        var channelOpts = options.ChannelOptions ?? new(options.BatchSize) { AllowSynchronousContinuations = false, SingleReader = true, SingleWriter = targetOpts.MaxDegreeOfParallelism == 1 };

        targetOpts.CancellationToken = sourceOpts.CancellationToken = token;

        var channel = Channel.CreateBounded<TFrom>(channelOpts);
        var (writer, reader) = (channel.Writer, channel.Reader);

        var target = new ActionBlock<TFrom>(async m => 
        {
            try
            {
                if (!writer.TryWrite(m)) await writer.WriteAsync(m);
            }
            catch (Exception ex)
            {
                if (options.CompleteOnError) channel.Writer.TryComplete();
                onError(ex);
                if (options.CompleteOnError) throw;
            }
        }, targetOpts);

        var source = new TransformBlock<TTo[], TTo[]>(t => t, sourceOpts);

        target.Completion.ContinueWith(t => channel.Writer.TryComplete(), token);
        
        _ = Task.Run(async () => 
        {
            var (batch, written) = (new TTo[options.BatchSize], 0);
            try
            {
                while (await reader.WaitToReadAsync(token))
                {
                    while (reader.TryRead(out var item))
                    {
                        batch[written++] = mapper(item);
                        if (written == batch.Length) (_, written) = (await Post(source, batch.AsSpan(0, written).ToArray()), 0);
                    }

                    if (written != 0) (_, written) = (await Post(source, batch.AsSpan(0, written).ToArray()), 0);
                }
                    
                if (written != 0) await Post(source, batch.AsSpan(0, written).ToArray());
            }
            catch (OperationCanceledException) 
            {
                source.Complete();
            }
            catch (Exception ex)
            {
                if (options.CompleteOnError) source.Complete();
                onError(ex);
            }
        }, token);

        return DataflowBlock.Encapsulate(target, source);

        async Task<bool> Post(ITargetBlock<TTo[]> target, TTo[] output) // bool for fancy syntax
        {
            while (true)
            {
                if (target.Post(output)) return true;
                await Task.Delay(options.OfferPause ?? TimeSpan.FromMilliseconds(500), token);
            }
        }
    }
}