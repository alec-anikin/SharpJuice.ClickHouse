using SharpJuice.Clickhouse.TableSchema;

namespace SharpJuice.Clickhouse.Octonica;

internal sealed class OctonicaTableSinkFactory : ITableSinkFactory
{
    private readonly IClickHouseConnectionFactory _connectionFactory;

    public OctonicaTableSinkFactory(IClickHouseConnectionFactory connectionFactory)
        => _connectionFactory = connectionFactory;

    public ITableSink Create(string tableName, IReadOnlyList<string> columnNames)
        => new OctonicaTableSink(
            _connectionFactory,
            $"insert into {tableName}({string.Join(", ", columnNames)}) values");
}

internal sealed class OctonicaTableSink : ITableSink
{
    private readonly IClickHouseConnectionFactory _connectionFactory;
    private readonly string _insertCommand;

    public OctonicaTableSink(IClickHouseConnectionFactory connectionFactory, string insertCommand)
    {
        _connectionFactory = connectionFactory;
        _insertCommand = insertCommand;
    }

    public async Task Write(ITable table, CancellationToken cancellationToken)
    {
        var columns = new Dictionary<string, object?>(table.Columns.SelectMany(c => c.GetValues()));

        await using var connection = _connectionFactory.Create();
        await connection.OpenAsync(cancellationToken);

        await using var writer = await connection.CreateColumnWriterAsync(_insertCommand, cancellationToken);

        await writer.WriteTableAsync(columns, table.RowCount, cancellationToken);
    }
}
