using SharpJuice.Clickhouse.TableSchema;

namespace SharpJuice.Clickhouse;

internal interface ITableBuilder<T>
{
    ITable CreateTable(ReadOnlySpan<T> records);

    ITable CreateTable(IEnumerable<T> records);
}