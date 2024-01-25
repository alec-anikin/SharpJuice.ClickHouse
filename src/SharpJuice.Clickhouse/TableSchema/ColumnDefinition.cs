namespace SharpJuice.Clickhouse.TableSchema;

internal sealed class ColumnDefinition<TRecord, TColumn>(string name, Func<TRecord, TColumn> getValue) : IColumnDefinition<TRecord>
{
    public IEnumerable<string> GetNames()
    {
        yield return name;
    }

    public IColumn<TRecord> CreateColumn(int recordsCount)
        => new Column<TRecord, TColumn>(name, recordsCount, getValue);
}

internal sealed class ArrayColumnDefinition<TRecord, TColumn>(string name, Func<TRecord, TColumn> getValue)
    : IArrayColumnDefinition<TRecord>
{
    public string Name { get; } = name;

    public IArrayColumn<TRecord> CreateColumn(int recordsCount, int estimatedArraySize)
        => new ArrayColumn<TRecord, TColumn>(Name, recordsCount, estimatedArraySize, getValue);
}