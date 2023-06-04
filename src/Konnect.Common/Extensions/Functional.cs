namespace Konnect.Common.Extensions;

public static class Actions<T>
{
    public static readonly Action<T> Identity1 = _ => {};
}

public static class FunctionalExtensions
{
    public static TMap Map<T, TMap, TContext>(this T value, TContext ctx, Func<T, TContext, TMap> map) => map(value, ctx);

    public static TMap Map<T, TMap>(this T value, Func<T, TMap> map) => map(value);

    public static T Tap<T>(this T value, Action<T> tap)
    {
        tap(value);
        return value;
    }

    public static T Tap<T, TContext>(this T value, TContext ctx, Action<T, TContext> tap)
    {
        tap(value, ctx);
        return value;
    }

    public static Action<T> Compose<T>(this IEnumerable<Action<T>> actions) => v =>
    {
        foreach (var action in actions) action(v);
    };

    public static Func<T, bool> Compose<T>(this IEnumerable<Func<T, bool>> funcs) => v => 
    {
        foreach (var func in funcs)
            if (!func(v)) return false;

        return true;
    };

    public static Func<T, T> Compose<T>(this IEnumerable<Func<T, T>> funcs) => v => 
    {
        foreach (var func in funcs) v = func(v);
        return v;
    };
}