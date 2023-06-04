using Konnect.Common;
using Konnect.Mongo;
using Konnect.Mongo.Contracts;

namespace Konnect.Processing.Common.Processors;


public sealed class MongoEventTypesProcessor : IProcessor
{
    readonly Configuration _config;
    readonly ObservabilityContext _obs;
    
    public MongoEventTypesProcessor(ObservabilityContext obs, Configuration config)
    {
        _obs = obs;
        _config = config;
        _config.Supported = _config.Supported switch
        {
            null or { Count: 0 } => Configuration.DefaultSupported,
            var c => c
        };
    }

    public ValueTask<ProcessingResult> Process(IMongoEvent @event, CancellationToken token)
    {
        if (_config.Supported.Contains(@event.Type)) return new(new ProcessingResult(ResultType.Ok, @event));
        return new(new ProcessingResult(ResultType.FilteredOut, @event));
    }

    public sealed class Configuration
    {
        public HashSet<MongoEventType> Supported { get; set; } = new();

        public static readonly HashSet<MongoEventType> DefaultSupported = new()
        {
            MongoEventType.Insert,
            MongoEventType.Replace,
            MongoEventType.Update,
            MongoEventType.Delete
        };
    }
}