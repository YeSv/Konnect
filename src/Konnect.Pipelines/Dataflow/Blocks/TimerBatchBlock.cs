using System.Threading.Tasks.Dataflow;
using Konnect.Common.Extensions;

namespace Konnect.Pipelines.Dataflow.Blocks;

public static class TimerBatchBlock
{
    public static IPropagatorBlock<T, T[]> Create<T>(
        int batchSize,
        int numOfBatches,
        TimeSpan flushTimeout,
        CancellationToken token = default)
    {
        var batchBlock = new BatchBlock<T>(batchSize, new()
        {
            CancellationToken = token,
            BoundedCapacity = batchSize * numOfBatches
        });

        var timer = new Timer(_ =>
        {
            try
            {
                batchBlock.TriggerBatch();
            }
            catch { }
        }, null, flushTimeout, flushTimeout);

        var transformBlock = new TransformBlock<T[], T[]>(
            b => timer.Change(flushTimeout, flushTimeout).Map(b, (t, b) => b), 
            new()
            {
                BoundedCapacity = 1,
                CancellationToken = token,
                SingleProducerConstrained = true
            });

        batchBlock.LinkTo(transformBlock, new() { PropagateCompletion = true });
        transformBlock.Completion.ContinueWith(_ => timer.Dispose());

        return DataflowBlock.Encapsulate(batchBlock, transformBlock);
    }
}