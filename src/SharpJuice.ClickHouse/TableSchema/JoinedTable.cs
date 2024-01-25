namespace SharpJuice.Clickhouse.TableSchema;

internal sealed class JoinedTable<TRecord, TItem> : ITable
{
    private readonly IColumn<TRecord>[] _columns;
    private readonly IColumn<TItem>[] _itemColumns;
    private readonly Func<TRecord, IReadOnlyCollection<TItem>> _getItems;

    public JoinedTable(
        int rowCount,
        IColumn<TRecord>[] columns,
        IColumn<TItem>[] itemColumns,
        Func<TRecord, IReadOnlyCollection<TItem>> getItems)
    {
        _columns = columns;
        _itemColumns = itemColumns;
        _getItems = getItems;
        RowCount = rowCount;
    }

    public int RowCount { get; }

    public IEnumerable<IColumn> Columns
    {
        get
        {
            IEnumerable<IColumn> columns = _columns;

            return columns.Concat(_itemColumns);
        }
    }

    public void AddRecord(TRecord record)
    {
        var items = _getItems(record);
        var count = items.Count;

        if (items == null || count == 0)
            throw new InvalidOperationException("Items is empty");

        foreach (var column in _columns)
            column.AddValue(record, repeat: count);

        foreach (var item in items)
        foreach (var itemColumn in _itemColumns)
            itemColumn.AddValue(item);
    }

    public void Dispose()
    {
        foreach (var column in _columns)
            column.Dispose();

        foreach (var column in _itemColumns)
            column.Dispose();
    }
}