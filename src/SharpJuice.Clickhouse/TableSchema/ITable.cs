namespace SharpJuice.Clickhouse.TableSchema;

internal interface ITable : IDisposable
{
    int RowCount { get; }
    IEnumerable<IColumn> Columns { get; }
}