using SharpJuice.Clickhouse.TableSchema;

namespace SharpJuice.Clickhouse;

internal interface ITableSink
{
    Task Write(ITable table, CancellationToken cancellationToken);
}

internal interface ITableSinkFactory
{
    ITableSink Create(string tableName, IReadOnlyList<string> columnNames);
}
