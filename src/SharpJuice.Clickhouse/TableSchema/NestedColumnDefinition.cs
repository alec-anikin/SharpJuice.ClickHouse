namespace SharpJuice.Clickhouse.TableSchema;

internal sealed class NestedColumnDefinition<TRecord, TItem> : IColumnDefinition<TRecord>
{
    private readonly CollectionAccessor<TRecord, TItem> _collectionAccessor;
    private readonly IArrayColumnDefinition<TItem>[] _innerColumns;
    private readonly ArrayStatistics _stat = new();
    private readonly string _name;

    public NestedColumnDefinition(
        string name,
        CollectionAccessor<TRecord, TItem> collectionAccessor,
        IArrayColumnDefinition<TItem>[] innerColumns)
    {
        _collectionAccessor = collectionAccessor;
        _innerColumns = innerColumns;
        _name = name;
    }

    public IEnumerable<string> GetNames()
    {
        var prefix = string.IsNullOrWhiteSpace(_name)
            ? string.Empty
            : _name + ".";

        foreach (var column in _innerColumns)
            yield return prefix + column.Name;
    }

    public IColumn<TRecord> CreateColumn(int recordsCount)
    {
        var arrayColumns = new IArrayColumn<TItem>[_innerColumns.Length];

        for (var i = 0; i < _innerColumns.Length; i++)
            arrayColumns[i] = _innerColumns[i].CreateColumn(recordsCount, _stat.GetAverageSize());

        return new NestedColumn<TRecord, TItem>(_name, _collectionAccessor, arrayColumns, _stat);
    }
}