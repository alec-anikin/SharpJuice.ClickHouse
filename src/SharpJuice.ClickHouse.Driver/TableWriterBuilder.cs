namespace SharpJuice.Clickhouse.Driver;

public sealed class TableWriterBuilder : ITableWriterBuilder
{
    private readonly Clickhouse.TableWriterBuilder _inner;

    public TableWriterBuilder(IClickHouseConnectionFactory connectionFactory)
        => _inner = new Clickhouse.TableWriterBuilder(new DriverTableSinkFactory(connectionFactory));

    public ITableWriterBuilder<TSource> For<TSource>(string tableName)
        => _inner.For<TSource>(tableName);
}
