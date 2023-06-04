namespace Konnect.Kafka;

public sealed class KafkaClusterConfiguration
{
    public string Name { get; set; } = string.Empty;
    public Dictionary<string, string> Config { get; set; } = new();
}

public sealed class ConnectionConfiguration
{
    public string Type { get; set; } = string.Empty;
    public string[] Topics { get; set; } = Array.Empty<string>();
    public Dictionary<string, string> Config { get; set; } = new();
}

public sealed class SinkConfiguration
{
    public ConnectionConfiguration Connection { get; set; } = new();   
}

public sealed class SourceConfiguration 
{
    public int BatchSize { get; set; } = 1_000;
    public ConnectionConfiguration Connection { get; set; } = new();
    public TimeSpan ConsumeDuration { get; set; } = TimeSpan.FromMilliseconds(100);
}
