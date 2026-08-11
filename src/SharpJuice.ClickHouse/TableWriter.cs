using System.Runtime.InteropServices;
using SharpJuice.Clickhouse.TableSchema;

namespace SharpJuice.Clickhouse;

internal sealed class TableWriter<T> : ITableWriter<T>
{
    private readonly ITableBuilder<T> _tableBuilder;
    private readonly ITableSink _sink;

    public TableWriter(ITableBuilder<T> tableBuilder, ITableSink sink)
    {
        _tableBuilder = tableBuilder;
        _sink = sink;
    }

    public Task Insert(ReadOnlySpan<T> records, CancellationToken cancellationToken = default)
    {
        if (records.Length == 0)
            return Task.CompletedTask;

        return InsertSpan(_tableBuilder.CreateTable(records), cancellationToken);

        async Task InsertSpan(ITable table, CancellationToken token)
        {
            using (table)
            {
                await _sink.Write(table, token);
            }
        }
    }

    public Task Insert(T[] records, CancellationToken token = default)
        => Insert(new ReadOnlySpan<T>(records), token);

    public Task Insert(IEnumerable<T> records, CancellationToken token = default)
    {
        return records switch
        {
            T[] array => Insert(new ReadOnlySpan<T>(array), token),
            List<T> list => Insert(CollectionsMarshal.AsSpan(list), token),
            _ => InsertEnumerable(records, token)
        };
    }

    private async Task InsertEnumerable(
        IEnumerable<T> records,
        CancellationToken token)
    {
        using var table = _tableBuilder.CreateTable(records);

        if(table.RowCount == 0)
            return;

        await _sink.Write(table, token);
    }
}
