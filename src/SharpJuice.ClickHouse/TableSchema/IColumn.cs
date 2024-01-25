namespace SharpJuice.Clickhouse.TableSchema;

internal interface IColumn<TRecord> : IColumn
{
    void AddValue(in TRecord record, int repeat = 1);
}

internal interface IColumn : IDisposable
{
    IEnumerable<KeyValuePair<string, object?>> GetValues();
}

internal interface IArrayColumn<TItem> : IDisposable
{
    void StartArray(int length);

    void AddValue(in TItem record);

    string Name { get; }

    object? GetValues();
}