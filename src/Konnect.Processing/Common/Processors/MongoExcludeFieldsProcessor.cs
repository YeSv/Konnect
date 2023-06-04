
using Konnect.Common;
using Konnect.Mongo;
using Konnect.Mongo.Contracts;
using MongoDB.Bson;
using Simple.Dotnet.Utilities.Buffers;

namespace Konnect.Processing.Common.Processors;

public sealed class MongoExcludeFieldsProcessor : IProcessor
{
    readonly Configuration _config;
    readonly ObservabilityContext _obs;

    public MongoExcludeFieldsProcessor(ObservabilityContext obs, Configuration config)
    {
        _obs = obs;
        _config = config;
    }

    public ValueTask<ProcessingResult> Process(IMongoEvent @event, CancellationToken token) 
    {
        if (_config.Fields is null or { Count: 0 }) 
            return new(new ProcessingResult(ResultType.Ok, @event));
        
        return new(@event switch
        {
            InsertEvent i => Process(i, _config.Fields),
            UpdateEvent u => Process(u, _config.Fields),
            ReplaceEvent r => Process(r, _config.Fields),
            var e => new(ResultType.Ok, e)
        });
    }

    static ProcessingResult Process(InsertEvent @event, HashSet<string> fields)
    {
        @event.Document = ExcludeDocumentFields(@event.Document, fields);
        return new(ResultType.Ok, @event);
    }

    static ProcessingResult Process(ReplaceEvent @event, HashSet<string> fields)
    {
        @event.Document = ExcludeDocumentFields(@event.Document, fields);
        return new(ResultType.Ok, @event);
    }

    static ProcessingResult Process(UpdateEvent @event, HashSet<string> fields)
    {
        if (@event.Document != null) // On full document
        {
            @event.Document = ExcludeDocumentFields(@event.Document, fields);
            return new ProcessingResult(ResultType.Ok, @event);
        }

        if (@event.Update != null) @event.Update = ExcludeUpdateFields(@event.Update, fields);
        if (@event.RemovedFields != null) @event.RemovedFields = ExcludeRemovedFields(@event.RemovedFields, fields);

        if (@event.Update == null && @event.RemovedFields == null) 
            return new ProcessingResult(ResultType.FilteredOut, @event);

        return new ProcessingResult(ResultType.Ok, @event);
    }

    static BsonDocument ExcludeDocumentFields(BsonDocument document, HashSet<string> fields)
    {
        foreach (var fieldName in fields)
        {
            if (document.Contains(fieldName)) document.Remove(fieldName);
        }

        return document;
    }

    static BsonDocument? ExcludeUpdateFields(BsonDocument update, HashSet<string> fields)
    {
        using var fieldsToRemove = new Rent<string>(update.ElementCount);
        foreach (var field in fields)
        {
            foreach (var prop in update)
            {
                if (prop.Name == field || prop.Name.Contains(field)) fieldsToRemove.Append(prop.Name);
            }
        }

        for (var i = 0; i < fieldsToRemove.Written; i++) update.Remove(fieldsToRemove.WrittenSpan[i]);

        if (update.ElementCount == 0) return null;

        return update;
    }

    static string[]? ExcludeRemovedFields(string[] removedFields, HashSet<string> fields)
    {
        using var fieldsToRemove = new Rent<string>(removedFields.Length);
        foreach (var removed in removedFields)
        {
            if (fields.Contains(removed)) fieldsToRemove.Append(removed);
        }

        if (fieldsToRemove.Written == 0) return removedFields;
        if (fieldsToRemove.Written < removedFields.Length) return removedFields.Where(f => !fields.Contains(f)).ToArray();

        return null;
    }

    public sealed class Configuration
    {
        public HashSet<string> Fields { get; set; } = new();
    }
}