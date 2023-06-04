namespace Konnect.Mongo;

public enum ChangeStreamMode { UseToken, FromTimestamp, Reset }

public sealed class MongoClusterConfiguration
{
    public string Name { get; set; } = string.Empty;
    public string ConnectionString { get; set; } = string.Empty;
}

public sealed class ConnectionConfiguration
{
    public string Type { get; set; } = string.Empty;
    public string Database { get; set; } = string.Empty;
}

public sealed class ChangeStreamConfiguration
{
    public int BatchSize { get; set; } = 10_000;
    public bool FullDocument { get; set; }
    public ChangeStreamMode Mode { get; set; }
    public string? Token { get; set; }
    public long? Timestamp { get; set; }
    public string Collection { get; set; } = string.Empty;
    public string[] Pipeline { get; set; } = Array.Empty<string>();
}

public sealed class UpsertsConfiguration
{
    public bool InsertAsReplace { get; set; } = true;
    public bool UpsertOnUpdate { get; set; } = false;
    public bool UpsertOnReplace { get; set; } = true;
}

public sealed class SinkConfiguration 
{
    public bool IsOrdered { get; set; } = true;
    public string Collection { get; set; } = string.Empty;
    public UpsertsConfiguration Upserts { get; set; } = new();
    public ConnectionConfiguration Connection { get; set; } = new();
}

public sealed class SourceConfiguration 
{
    public ConnectionConfiguration Connection { get; set; } = new();
    public ChangeStreamConfiguration ChangeStream { get; set; } = new();
}