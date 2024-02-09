using AutoFixture;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Dapper;
using SharpJuice.Clickhouse;
using SharpJuice.Clickhouse.Tests.Infrastructure;

namespace Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class Insert
{
    private ClickHouseDatabaseSettings _databaseSettings = null!;
    private ClickHouseConnectionFactory _connectionFactory = null!;
    private ITableWriter<TestObject> _nestedWriter = null!;
    private ITableWriter<TestObject> _plainWriter = null!;
    private TestObject _testObject = null!;

    [Params(100, 1000, 10000, 30000)]
    public int ObjectsCount { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var configuration = new ClickHouseConfiguration();
        _databaseSettings = configuration.CreateDatabase();
        _connectionFactory = new ClickHouseConnectionFactory(_databaseSettings.ConnectionSettings);

        var builder = new TableWriterBuilder(_connectionFactory);

        _nestedWriter = builder.For<TestObject>("bench_table")
            .AddColumn("order_id", a => a.OrderId)
            .AddColumn("cart_id", a => a.CartId)
            .AddColumn("total_amount", a => a.TotalAmount)
            .AddColumn("date", a => a.Date)
            .AddNestedColumn("item", x => x.Items, c => c
                .AddColumn("id", x => x.Id)
                .AddColumn("quantity", x => x.Quantity)
                .AddColumn("name", x => x.Name)
                .AddColumn("price", x => x.Price))
            .AddNestedColumn(x => x.Discounts, c => c
                .AddColumn("discount.id", x => x.Id)
                .AddColumn("discount.name", x => x.Name)
                .AddColumn("discount.value", x => x.Value))
            .AddNestedColumn("benefit", x => x.Benefits, c => c
                .AddColumn("code", x => x.Code)
                .AddColumn("value", x => x.Value))
            .Build();

        _plainWriter = builder.For<TestObject>("bench_table")
            .AddColumn("order_id", a => a.OrderId)
            .AddColumn("cart_id", a => a.CartId)
            .AddColumn("total_amount", a => a.TotalAmount)
            .AddColumn("date", a => a.Date)
            .AddColumn("item.id", x => x.Items.Select(x => x.Id).ToArray())
            .AddColumn("item.quantity", x => x.Items.Select(x => x.Quantity).ToArray())
            .AddColumn("item.name", x => x.Items.Select(x => x.Name).ToArray())
            .AddColumn("item.price", x => x.Items.Select(x => x.Price).ToArray())
            .AddColumn("discount.id", x => x.Discounts.Select(x => x.Id).ToArray())
            .AddColumn("discount.name", x => x.Discounts.Select(x => x.Name).ToArray())
            .AddColumn("discount.value", x => x.Discounts.Select(x => x.Value).ToArray())
            .AddColumn("benefit.code", x => x.Benefits.Select(x => x.Code).ToArray())
            .AddColumn("benefit.value", x => x.Benefits.Select(x => x.Value).ToArray())
            .Build();

        await using var connection = _databaseSettings.CreateConnection(string.Empty);

        await connection.ExecuteAsync(
            $"CREATE DATABASE IF NOT EXISTS {_databaseSettings.ConnectionSettings.Database}");

        await CreateTable();

        var fixture = new Fixture { RepeatCount = 5 };
        fixture.Inject(new DateOnly(2022, 9, 14));

        _testObject = fixture.Create<TestObject>();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        using var connection = _connectionFactory.Create();
        connection.Execute($"DROP DATABASE IF EXISTS {_databaseSettings.ConnectionSettings.Database}");
    }

    [Benchmark]
    public Task FlatObject_Writer()
    {
        var records = Enumerable.Repeat(_testObject, ObjectsCount);

        return _plainWriter.Insert(records, CancellationToken.None);
    }

    [Benchmark]
    public Task NestedObject_Writer()
    {
        var records = Enumerable.Repeat(_testObject, ObjectsCount);

        return _nestedWriter.Insert(records, CancellationToken.None);
    }

    [Benchmark]
    public async Task ClickhouseClient_ColumnWriter()
    {
        var records = Enumerable.Repeat(_testObject, ObjectsCount).ToArray();

        const string insertCommand =
            "insert into bench_table(order_id, cart_id, total_amount, date, item.id, item.quantity, item.name, item.price, discount.id, discount.name, discount.value, benefit.code, benefit.value) values";
        await using var connection = _connectionFactory.Create();
        await connection.OpenAsync();

        await using var writer = await connection.CreateColumnWriterAsync(insertCommand, CancellationToken.None);

        var columns = new Dictionary<string, object?>
        {
            { "order_id", records.Select(r => r.OrderId).ToArray() },
            { "cart_id", records.Select(r => r.CartId).ToArray() },
            { "total_amount", records.Select(r => r.TotalAmount).ToArray() },
            { "date", records.Select(r => r.Date).ToArray() },
            { "item.id", records.Select(r => r.Items.Select(i => i.Id).ToArray()).ToArray() },
            { "item.quantity", records.Select(r =>  r.Items.Select(i => i.Quantity).ToArray()).ToArray() },
            { "item.name", records.Select(r => r.Items.Select(i => i.Name).ToArray()).ToArray() },
            { "item.price", records.Select(r => r.Items.Select(i => i.Price).ToArray()).ToArray() },
            { "discount.id", records.Select(r => r.Discounts.Select(i => i.Id).ToArray()).ToArray() },
            { "discount.name", records.Select(r => r.Discounts.Select(i => i.Name).ToArray()).ToArray() },
            { "discount.value", records.Select(r => r.Discounts.Select(i => i.Value).ToArray()).ToArray() },
            { "benefit.code", records.Select(r => r.Benefits.Select(i => i.Code).ToArray()).ToArray() },
            { "benefit.value", records.Select(r => r.Benefits.Select(i => i.Value).ToArray()).ToArray() }
        };

        await writer.WriteTableAsync(columns, records.Length, CancellationToken.None);
    }     

    public sealed record TestObject(
        long OrderId,
        int CartId,
        decimal TotalAmount,
        DateOnly Date,
        Item[] Items,
        IEnumerable<Discount> Discounts,
        List<Benefit> Benefits);

    public readonly record struct Item(int Id, int Quantity, string Name, decimal Price);

    public sealed record Discount(int Id, string Name, decimal Value);

    public sealed record Benefit(Guid Code, int Value);

    private async Task CreateTable()
    {
        const string createSql = """
            CREATE TABLE IF NOT EXISTS bench_table
            (
                order_id Int64,
                cart_id Int32,
                total_amount Decimal(18,6),
                date Date,
                item Nested (
                    id Int32,
                    quantity Int32,
                    name String,
                    price Decimal(18,6)
                ),
                discount Nested (
                    id Int32,
                    name String,
                    value Decimal(18,6)
                ),
                benefit Nested (
                    code UUID,
                    value Int32
                )
            ) ENGINE = MergeTree()
            ORDER BY order_id;
            """;

        await using var connection = _connectionFactory.Create();
        await connection.ExecuteAsync(createSql);
    }
}