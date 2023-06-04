using System.Threading.Channels;
using Konnect.Common.Extensions;
using Simple.Dotnet.Utilities.Buffers;

namespace Konnect.Pipelines.Dataflow.Channels;

public sealed class BatchingChannel
{
    public static (ChannelWriter<T> Writer, Task ReaderTask) Create<T>(
        int capacity,
        int batchSize,
        Func<ReadOnlyMemory<T>, CancellationToken, Task> onBatch,
        Action<Exception> onError,
        CancellationToken token)
    {
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
        {
            SingleReader = true,
            SingleWriter = true
        });

        var (writer, reader) = (channel.Writer, channel.Reader);
        var readerTask = Task.Run(async () =>
        {
            try
            {
                using var batch = new Rent<T>(batchSize);
                while (!token.IsCancellationRequested)
                {
                    while (await reader.WaitToReadAsync(token))
                    {
                        try
                        {
                            while (reader.TryRead(out var item))
                            {
                                batch.Append(item);
                                if (batch.IsFull)
                                {
                                    await onBatch(batch.WrittenMemory, token);
                                    batch.Clear();
                                    continue;
                                }
                            }

                            if (batch.HasSome) await onBatch(batch.WrittenMemory, token);
                        }
                        finally
                        {
                            batch.Clear();
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                onError(ex);
            }
        }, token);

        return (writer, readerTask);
    }
}
