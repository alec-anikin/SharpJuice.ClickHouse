using Octonica.ClickHouseClient;
using System.Runtime.InteropServices;
using SharpJuice.Clickhouse.TableSchema;

namespace SharpJuice.Clickhouse;

internal sealed class TableWriter<T> : ITableWriter<T>
{
    private readonly ITableBuilder<T> _tableBuilder;
    private readonly IClickHouseConnectionFactory _connectionFactory;
    private readonly string _insertCommand;

    public TableWriter(
        ITableBuilder<T> tableBuilder,
        string insertCommand,
        IClickHouseConnectionFactory connectionFactory)
    {
        _tableBuilder = tableBuilder;
        _insertCommand = insertCommand;
        _connectionFactory = connectionFactory;
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
                await Insert(table, token);
            }
        }
    }

    public Task Insert(
        IEnumerable<T> records,
        CancellationToken token)
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

        await Insert(table, token);
    }

    private async Task Insert(ITable table, CancellationToken cancellationToken)
    {
        var columns = new Dictionary<string, object?>(table.Columns.SelectMany(c => c.GetValues()));

        await using var connection = _connectionFactory.Create();
        await connection.OpenAsync(cancellationToken);

        await using var writer = await connection.CreateColumnWriterAsync(_insertCommand, cancellationToken);

        await writer.WriteTableAsync(columns, table.RowCount, cancellationToken);
    }
}