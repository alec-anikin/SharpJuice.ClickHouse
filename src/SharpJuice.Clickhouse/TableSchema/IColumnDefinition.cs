namespace SharpJuice.Clickhouse.TableSchema;

internal interface IColumnDefinition<TRecord>
{
    IEnumerable<string> GetNames();

    IColumn<TRecord> CreateColumn(int recordsCount);
}

internal interface IArrayColumnDefinition<TRecord>
{
    string Name { get; }

    IArrayColumn<TRecord> CreateColumn(int recordsCount, int estimatedArraySize);
}