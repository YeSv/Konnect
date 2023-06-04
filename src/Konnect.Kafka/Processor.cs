using Confluent.Kafka;

namespace Konnect.Kafka;

public enum ResultType : byte { FilteredOut, Ok }

public readonly record struct ProcessingResult(ResultType Type, Message<byte[], byte[]> Message);

public delegate ValueTask<ProcessingResult> Processor(Message<byte[], byte[]> message, CancellationToken token);

public interface IProcessor
{
    public ValueTask<ProcessingResult> Process(Message<byte[], byte[]> message, CancellationToken token);
}

public static class Processors
{
    public static readonly Processor Identity = (m, t) => new(new ProcessingResult(ResultType.Ok, m));

    public static Processor Compose(this Processor[] processors) 
    {
        if (processors.Length == 0) return Identity;
        return async (message, token) =>
        {
            var result = new ProcessingResult(ResultType.Ok, message);
            foreach (var processor in processors)
            {
                token.ThrowIfCancellationRequested();

                result = await processor(result.Message, token);
                if (result.Type is not ResultType.Ok) break;
            }

            return result;
        };
    }
}
