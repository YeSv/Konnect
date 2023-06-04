using Konnect.Actions;
using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration;

public static class Sections
{
    public static IEnumerable<IConfigurationSection> GetActionSections(this IConfigurationSection instance) =>
        instance.GetSection("actions").GetChildren();

    public static IConfigurationSection? GetActionSection(this IConfigurationSection instance, string name) =>
        instance.GetActionSections().FirstOrDefault(s => s.Read<string>("configuration:name") == name);

    public static IEnumerable<string> ReadActiveActions(this IConfigurationSection instance) =>
        instance.GetActionSections().Select(s => s.ReadConfiguration<ActionConfiguration>()!).Where(s => s.Active).Select(s => s.Name);

    public static IConfigurationSection GetHealthSection(this IConfiguration configuration) => configuration.GetSection("health");

    public static IConfigurationSection GetChecksSections(this IConfigurationSection health) => health.GetSection("checks");

    public static IConfigurationSection GetProcessorsSection(this IConfigurationSection pipeline) => pipeline.GetSection("processors");
    
    public static IConfigurationSection GetPartitionerSection(this IConfigurationSection pipeline) => pipeline.GetSection("partitioner");

    public static IConfigurationSection GetPipelineSection(this IConfigurationSection action) => action.GetSection("pipeline");

    public static IConfigurationSection GetStoragesSection(this IConfigurationSection instance) => instance.GetSection("storages");

    public static string[] ReadKafkas(this IConfigurationSection storages) => storages.Read<string[]>("kafkas");
    public static string[] ReadMongos(this IConfigurationSection storages) => storages.Read<string[]>("mongos");

    public static T ReadConfiguration<T>(this IConfigurationSection section) where T : class => section.Read<T>("configuration");

    public static T ReadOptionalConfiguration<T>(this IConfigurationSection section) where T : class, new() => section.ReadOptional<T>("configuration");

    public static T ReadSinkConfiguration<T>(this IConfigurationSection pipeline) where T : class => pipeline.GetSection("sink").ReadConfiguration<T>();

    public static T ReadSourceConfiguration<T>(this IConfigurationSection pipeline) where T : class => pipeline.GetSection("source").ReadConfiguration<T>();

    public static T ReadOffsetsConfiguration<T>(this IConfigurationSection pipeline) where T : class => pipeline.GetSection("offsets").ReadConfiguration<T>();
}
