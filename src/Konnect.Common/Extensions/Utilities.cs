using Simple.Dotnet.Utilities.Buffers;

namespace Konnect.Common.Extensions;

public readonly record struct Bytes(ReadOnlyMemory<byte> Memory)
{
    public bool Equals(Bytes bytes) => bytes.GetHashCode() == GetHashCode();

    public readonly override int GetHashCode()
    {
        var code = new HashCode();
        code.AddBytes(Memory.Span);

        return code.ToHashCode();
    }
}

public static class Utilities
{
    public static Rent<T> Clone<T>(this Rent<T> buffer)
    {
        var clone = new Rent<T>(buffer.Written);
        buffer.WrittenSpan.CopyTo(clone.GetSpan(buffer.Written));
        clone.Advance(buffer.Written);
        return clone;
    }

    public static Rent<T> CopyTo<T>(this IEnumerable<T> values, Rent<T> rent)
    {
        foreach (var v in values) rent.Append(v);
        return rent;
    }

    public static Rent<T> AsRent<T>(this IEnumerable<T> values, int size) =>
        new Rent<T>(size).Map(values, (r, v) => v.CopyTo(r));

    public static bool Equal<T>(this T[] first, T[] second) => first.AsSpan().SequenceEqual(second);
}