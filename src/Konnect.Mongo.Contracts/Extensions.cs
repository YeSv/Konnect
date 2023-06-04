
using MongoDB.Bson;

namespace Konnect.Mongo.Contracts;

public static class EventsExtensions
{
    public static BsonDocument? GetKey(this IMongoEvent? @event) => @event switch
    {
        InsertEvent i => i.Key,
        DeleteEvent d => d.Key,
        ReplaceEvent r => r.Key,
        UpdateEvent u => u.Key,
        _ => null,
    };
}