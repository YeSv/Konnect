namespace Konnect.Common.Extensions;

public static class EnumerableExtensions
{
    public static IEnumerable<T> TapEach<T>(this IEnumerable<T> sequence, Action<T> action) =>
        sequence.Select(s => s.Tap(action));
    
    public static IEnumerable<T> TapEach<T>(this IEnumerable<T> sequence, Action<T, int> action) =>
        sequence.Select((s, i) => s.Tap(i, action));

    public static IEnumerable<T> TapEach<T, TContext>(this IEnumerable<T> sequence, TContext ctx, Action<T, TContext> action) =>
        sequence.Select(s => s.Tap(ctx, action));

    public static IEnumerable<T> TapEach<T, TContext>(this IEnumerable<T> sequence, TContext ctx, Action<T, TContext, int> action) =>
        sequence.Select((s, i) => s.Tap(i, (s, i) => action(s, ctx, i)));

    public static ReadOnlyMemory<T> TapEach<T>(this ReadOnlyMemory<T> sequence, Action<T> action) 
    {
        var span = sequence.Span;
        for (var i = 0; i < span.Length; i++) action(span[i]);   

        return sequence;
    }
    
    public static ReadOnlyMemory<T> TapEach<T>(this ReadOnlyMemory<T> sequence, Action<T, int> action)
    {
        var span = sequence.Span;
        for (var i = 0; i < span.Length; i++) action(span[i], i);   

        return sequence;
    }

    public static ReadOnlyMemory<T> TapEach<T, TContext>(this ReadOnlyMemory<T> sequence, TContext ctx, Action<T, TContext> action)
    {
        var span = sequence.Span;
        for (var i = 0; i < span.Length; i++) action(span[i], ctx);   

        return sequence;
    }

    public static ReadOnlyMemory<T> TapEach<T, TContext>(this ReadOnlyMemory<T> sequence, TContext ctx, Action<T, TContext, int> action) 
    {
        var span = sequence.Span;
        for (var i = 0; i < span.Length; i++) action(span[i], ctx, i);   

        return sequence;
    }

    public static void Materialize<T>(this IEnumerable<T> sequence, Action<T>? action = null)
    {
        action ??= Actions<T>.Identity1;
        foreach (var e in sequence) action.Invoke(e);
    }
}