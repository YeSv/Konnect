using Microsoft.Extensions.Configuration;

namespace Konnect.Hosting.Configuration;

public sealed class ConfigurationException<T> : Exception
{
    public ConfigurationException(string path, Exception inner) : base($"Failed to extract {typeof(T)} from path: {path}", inner) {}
}

public static class Instances
{
    public static readonly string KonnectInstanceName = "Konnect_INSTANCE_NAME";

    public static string? GetTypeOf(this IConfigurationSection c) => GetTypeOf(c, true)!;

    public static string? GetTypeOf(this IConfigurationSection c, bool optional) => (optional, c["type"]) switch
    {
        (false, null) => throw new Exception($"Can't get type for a section. Path {c.Path}"),
        (_, var s) => s
    };
    
    public static string GetInstanceName(this IConfiguration c) => c[KonnectInstanceName] ?? "main";
    

    public static T Read<T>(this IConfiguration c, string name) where T : class => c.GetSection(name).Read<T>();

    public static T ReadOptional<T>(this IConfiguration c, string name) where T : class, new() => c.GetSection(name).Read<T>(() => new());

    public static T Read<T>(this IConfigurationSection section, string name) where T : class => section.GetSection(name).Read<T>();

    public static T ReadOptional<T>(this IConfigurationSection section, string name) where T : class, new() => section.GetSection(name).Read<T>(() => new());

    public static T Read<T>(this IConfigurationSection section, Func<T>? instantiator = null) where T : class
    {
        try
        {
            return section.Get<T>() switch
            {
                { } v => v,
                null when instantiator != null => instantiator(),
                _ => throw new Exception("Section value is null")
            };
        }
        catch (Exception ex)
        {
            throw new ConfigurationException<T>(section.Path, ex);
        }
    }
    
    public static IConfigurationSection GetInstanceConfiguration(this IConfiguration c) => c.GetSection($"application:{nameof(Konnect)}:instances").GetSection(c.GetInstanceName());
}