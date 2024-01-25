using SharpJuice.Clickhouse.TableSchema;

namespace SharpJuice.Clickhouse;

internal sealed class TableBuilder<T> : ITableBuilder<T>
{
    private readonly IColumnDefinition<T>[] _columnDefinitions;

    public TableBuilder(IEnumerable<IColumnDefinition<T>> columnDefinitions)
    {
        _columnDefinitions = columnDefinitions.ToArray();
    }

    public ITable CreateTable(ReadOnlySpan<T> records)
    {
        var table = CreateTable(records.Length);

        foreach (var record in records)
            table.AddRecord(record);

        return table;
    }

    public ITable CreateTable(IEnumerable<T> records)
    {
        var table = records switch
        {
            IReadOnlyCollection<T> collection => CreateTable(collection.Count),
            _ => records.TryGetNonEnumeratedCount(out var count) ? CreateTable(count) : null
        };

        if (table == null)
        {
            using var list = new PooledList<T>(512);

            foreach (var record in records)
                list.Add(record);

            return CreateTable(list.Span);
        }

        foreach (var record in records)
            table.AddRecord(record);

        return table;
    }

    private Table<T> CreateTable(int recordsCount)
    {
        var columns = new IColumn<T>[_columnDefinitions.Length];

        for (var index = 0; index < columns.Length; ++index)
            columns[index] = _columnDefinitions[index].CreateColumn(recordsCount);

        return new Table<T>(recordsCount, columns);
    }
}