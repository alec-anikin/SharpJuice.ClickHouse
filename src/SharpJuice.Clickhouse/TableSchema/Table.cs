namespace SharpJuice.Clickhouse.TableSchema;

internal sealed class Table<TRecord> : ITable
{
    private readonly IColumn<TRecord>[] _columns;

    public Table(int rowCount, IColumn<TRecord>[] columns)
    {
        _columns = columns;
        RowCount = rowCount;
    }

    public int RowCount { get; }

    public IEnumerable<IColumn> Columns
        => _columns;

    public void AddRecord(TRecord record)
    {
        foreach (var column in _columns)
            column.AddValue(record);
    }

    public void Dispose()
    {
        foreach (var column in _columns)
            column.Dispose();
    }
}