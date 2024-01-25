using System.Runtime.InteropServices;

namespace SharpJuice.Clickhouse.TableSchema;

internal sealed class NestedColumn<TRecord, TItem> : IColumn<TRecord>
{
    private readonly CollectionAccessor<TRecord, TItem> _collectionAccessor;
    private readonly IArrayColumn<TItem>[] _innerColumns;
    private readonly ArrayStatistics _statistics;
    private readonly string _name;

    public NestedColumn(
        string name,
        CollectionAccessor<TRecord, TItem> collectionAccessor,
        IArrayColumn<TItem>[] innerColumns,
        ArrayStatistics statistics)
    {
        _collectionAccessor = collectionAccessor;
        _innerColumns = innerColumns;
        _statistics = statistics;
        _name = name;
    }

    public void AddValue(in TRecord record, int repeat = 1)
    {
        if (repeat > 1) throw new NotSupportedException();

        if (_collectionAccessor.CanUseSpan)
        {
            AddValues(_collectionAccessor.GetSpan(record));
        }
        else
        {
            var items = _collectionAccessor.GetCollection(record);

            if (items is TItem[] array)
                AddValues(array);
            else if (items is List<TItem> list)
                AddValues(CollectionsMarshal.AsSpan(list));
            else
                AddValues(items, GetCount(items));
        }
    }

    private void AddValues(ReadOnlySpan<TItem> items)
    {
        _statistics.SetSize(items.Length);

        for (var i = 0; i < _innerColumns.Length; i++)
            _innerColumns[i].StartArray(items.Length);

        foreach (ref readonly var item in items)
        foreach (var innerColumn in _innerColumns)
        {
            innerColumn.AddValue(item);
        }
    }

    private void AddValues(IEnumerable<TItem> items, int count)
    {
        _statistics.SetSize(count);

        for (var i = 0; i < _innerColumns.Length; i++)
            _innerColumns[i].StartArray(count);

        foreach (var item in items)
        foreach (var innerColumn in _innerColumns)
        {
            innerColumn.AddValue(item);
        }
    }

    private int GetCount(IEnumerable<TItem> items)
    {
        if (items is IReadOnlyCollection<TItem> collection)
            return collection.Count;

        if (items.TryGetNonEnumeratedCount(out var count))
            return count;

        return items.Count();
    }

    public IEnumerable<KeyValuePair<string, object?>> GetValues()
    {
        var prefix = string.IsNullOrWhiteSpace(_name)
            ? string.Empty
            : _name + ".";

        foreach (var column in _innerColumns)
            yield return new(prefix + column.Name, column.GetValues());
    }

    public void Dispose()
    {
        foreach (var column in _innerColumns)
            column.Dispose();
    }
}