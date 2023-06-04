using Konnect.Mongo.Contracts;

namespace Konnect.Mongo;

public enum ResultType : byte { FilteredOut, Ok }

public readonly record struct ProcessingResult(ResultType Type, IMongoEvent Event);

public delegate ValueTask<ProcessingResult> Processor(IMongoEvent @event, CancellationToken token);

public interface IProcessor
{
    ValueTask<ProcessingResult> Process(IMongoEvent @event, CancellationToken token);
}

public static class Processors
{
    public static readonly Processor Identity = (e, t) => new(new ProcessingResult(ResultType.Ok, e));

    public static Processor Compose(this Processor[] processors) 
    {
        if (processors.Length == 0) return Identity;
        return async (@event, token) =>
        {
            var result = new ProcessingResult(ResultType.Ok, @event);
            foreach (var processor in processors)
            {
                token.ThrowIfCancellationRequested();

                result = await processor(result.Event, token);
                if (result.Type != ResultType.Ok) break;
            }

            return result;
        };
    }
}