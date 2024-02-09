namespace SharpJuice.Clickhouse;

public interface ITableWriter<T>
{
    Task Insert(ReadOnlySpan<T> data, CancellationToken cancellationToken = default);

    Task Insert(T[] data, CancellationToken cancellationToken = default);

    Task Insert(IEnumerable<T> records, CancellationToken token = default);
}