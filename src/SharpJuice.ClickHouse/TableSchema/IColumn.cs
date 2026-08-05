using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Types;

namespace SharpJuice.Clickhouse.TableSchema;

internal interface IColumn<TRecord> : IColumn
{
    void AddValue(in TRecord record, int repeat = 1);
}

internal interface IColumn : IDisposable
{
    IEnumerable<KeyValuePair<string, object?>> GetValues();

    IEnumerable<IClickHouseColumnWriter> CreateWriters(Func<string, IClickHouseColumnTypeInfo> getTypeInfo);
}

internal interface IArrayColumn<TItem> : IDisposable
{
    void StartArray(int length);

    void AddValue(in TItem record);

    string Name { get; }

    object? GetValues();

    IClickHouseColumnWriter CreateWriter(string columnName, IClickHouseColumnTypeInfo typeInfo);
}
