using SharpJuice.Clickhouse.TableSchema;

namespace SharpJuice.Clickhouse;

internal sealed class JoinedTableBuilder<T, TItem> : ITableBuilder<T>
{
    private readonly IColumnDefinition<T>[] _columnDefinitions;
    private readonly IColumnDefinition<TItem>[] _itemColumnDefinitions;
    private readonly Func<T, IReadOnlyCollection<TItem>> _getItems;

    public JoinedTableBuilder(
        IEnumerable<IColumnDefinition<T>> columnDefinitions,
        IEnumerable<IColumnDefinition<TItem>> itemColumnDefinitions,
        Func<T, IReadOnlyCollection<TItem>> getItems)
    {
        _columnDefinitions = columnDefinitions.ToArray();
        _itemColumnDefinitions = itemColumnDefinitions.ToArray();
        _getItems = getItems;
    }

    public ITable CreateTable(ReadOnlySpan<T> records)
    {
        var itemsCount = 0;
        foreach (var record in records)
            itemsCount += _getItems(record).Count;

        var table = CreateTable(itemsCount);

        foreach (var record in records)
            table.AddRecord(record);

        return table;
    }

    public ITable CreateTable(IEnumerable<T> records)
    {
        var recordCount = records switch
        {
            IReadOnlyCollection<T> collection => collection.Count,
            _ => records.TryGetNonEnumeratedCount(out var count) ? count : 512
        };

        using var list = new PooledList<T>(recordCount);
        var itemsCount = 0;

        foreach (var record in records)
        {
            list.Add(record);
            itemsCount += _getItems(record).Count;
        }

        var table = CreateTable(itemsCount);

        foreach (var record in list.Span)
            table.AddRecord(record);

        return table;
    }

    private JoinedTable<T, TItem> CreateTable(int itemsCount)
    {
        var columns = new IColumn<T>[_columnDefinitions.Length];

        for (var index = 0; index < columns.Length; ++index)
            columns[index] = _columnDefinitions[index].CreateColumn(itemsCount);

        var itemColumns = new IColumn<TItem>[_itemColumnDefinitions.Length];

        for (var index = 0; index < itemColumns.Length; ++index)
            itemColumns[index] = _itemColumnDefinitions[index].CreateColumn(itemsCount);

        return new JoinedTable<T, TItem>(itemsCount, columns, itemColumns, _getItems);
    }
}