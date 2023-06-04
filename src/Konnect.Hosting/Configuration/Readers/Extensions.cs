using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration.Readers;

public static class ReadersExtensions
{
    public static CheckFn[] ReadChecks(this IConfigurationSection health, IEnumerable<IConfigurationReader<CheckFn>> readers) =>
        health.GetChecksSections().ExtractFromChildren(readers).ToArray();

    public static T[] ReadProcessors<T>(this IConfigurationSection pipeline, IEnumerable<IConfigurationReader<T>> readers) where T : class =>
        pipeline.GetProcessorsSection().ExtractFromChildren(readers).ToArray();

    public static T[] ReadPreProcessors<T>(this IConfigurationSection pipeline, IEnumerable<IConfigurationReader<T>> readers) where T : class =>
        pipeline.GetProcessorsSection().GetSection("pre").ExtractFromChildren(readers).ToArray();

    public static T[] ReadPostProcessors<T>(this IConfigurationSection pipeline, IEnumerable<IConfigurationReader<T>> readers) where T : class =>
        pipeline.GetProcessorsSection().GetSection("post").ExtractFromChildren(readers).ToArray();

    public static PipelineFn ReadPipeline(this IConfigurationSection action, IEnumerable<IConfigurationReader<PipelineFn>> readers) => 
        action.GetPipelineSection().Extract(readers) ?? throw new ConfigurationException<PipelineFn>(action.Path, new("Pipeline section should be defined"));

    public static T ReadPartitioner<T>(this IConfigurationSection pipeline, IEnumerable<IConfigurationReader<T>> readers) where T : class => 
        pipeline.GetPartitionerSection().Extract(readers) ?? throw new ConfigurationException<PipelineFn>(pipeline.Path, new("Partitioner section should be defined"));

    static IEnumerable<T> ExtractFromChildren<T>(this IConfigurationSection section, IEnumerable<IConfigurationReader<T>> readers) where T : class =>
        section.GetChildren().Select(s => s.Extract(readers)).Where(v => v != null)!;

    static T? Extract<T>(this IConfigurationSection section, IEnumerable<IConfigurationReader<T>> readers) where T : class
    {
        foreach (var reader in readers)
        {
            var instance = reader.Read(section);
            if (instance != null) return instance;
        }

        return null;
    }
}