using System.Text.Json.Serialization;
using System.Text.Json;

namespace Konnect.Kafka;

public sealed class Statistics
{
    [JsonPropertyName("type")] public string Type { get; set; } = string.Empty;
    [JsonPropertyName("topics")] public Dictionary<string, Topic> Topics { get; set; } = new();

    [JsonPropertyName("time")] public int Time { get; set; }

    public bool IsConsumer => Type == "consumer";

    public sealed class Topic
    {
        [JsonPropertyName("topic")] public string TopicName { get; set; } = string.Empty;
        [JsonPropertyName("partitions")] public Dictionary<string, Partition> Partitions { get; set; } = new();
    }

    public sealed class Partition
    {
        [JsonPropertyName("desired")] public bool Desired { get; set; }
        [JsonPropertyName("partition")] public int PartitionNum { get; set; }
        [JsonPropertyName("consumer_lag")] public long ConsumerLag { get; set; }
        [JsonPropertyName("stored_offset")] public long StoredOffset { get; set; }
        [JsonPropertyName("committed_offset")] public long CommittedOffset { get; set; }
        [JsonPropertyName("consumer_lag_stored")] public long ConsumerLagStored { get; set; }
    }

    public static (Statistics?, Exception?) FromJson(string json)
    {
        try
        {
            var statistics = JsonSerializer.Deserialize<Statistics?>(json);
            return (statistics, default);
        }
        catch (Exception ex)
        {
            return (default, ex);
        }
    }
}