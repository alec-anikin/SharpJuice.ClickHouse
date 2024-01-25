using AutoFixture;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using Dapper;
using SharpJuice.Clickhouse;
using SharpJuice.Clickhouse.Tests.Infrastructure;

namespace Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class InsertMemory
{
    private ClickHouseDatabaseSettings _databaseSettings = null!;
    private ClickHouseConnectionFactory _connectionFactory = null!;
    private Dictionary<string, object?> _objectsArray = null!;
    private Dictionary<string, object?> _objectsMemory = null!;
    private Dictionary<string, object?> _objectsReadonlyMemory = null!;
    private string _insertCommand = null!;
    private const int RowCount = 1000;

    [GlobalSetup]
    public async Task Setup()
    {
        var configuration = new ClickHouseConfiguration();
        _databaseSettings = configuration.CreateDatabase();
        _connectionFactory = new ClickHouseConnectionFactory(_databaseSettings.ConnectionSettings);

        await using var connection = _databaseSettings.CreateConnection(string.Empty);

        await connection.ExecuteAsync(
            $"CREATE DATABASE IF NOT EXISTS {_databaseSettings.ConnectionSettings.Database}");

        await CreateTable();

        var fixture = new Fixture { RepeatCount = 5 };
        fixture.Inject(new DateOnly(2022, 9, 14));
        fixture.RepeatCount = 20;

        var array = fixture.CreateMany<TestObject>(RowCount).ToArray();
        var memory = fixture.CreateMany<TestObjectMemory>(RowCount).ToArray();
        var readonlyMemory = fixture.CreateMany<TestObjectReadonlyMemory>(RowCount).ToArray();

        _objectsArray = new Dictionary<string, object?>
        {
            {"order_id", array.Select(x => x.OrderId).ToArray()},
            {"item.id", array.Select(x => x.Items).ToArray()}
        };

        _objectsMemory = new Dictionary<string, object?>
        {
            {"order_id", memory.Select(x => x.OrderId).ToArray()},
            {"item.id", memory.Select(x => x.Items).ToArray()}
        };

        _objectsReadonlyMemory = new Dictionary<string, object?>
        {
            {"order_id", readonlyMemory.Select(x => x.OrderId).ToArray()},
            {"item.id", readonlyMemory.Select(x => x.Items).ToArray()}
        };

        _insertCommand = $"insert into bench_table(order_id, item.id) values";
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        using var connection = _connectionFactory.Create();
        connection.Execute($"DROP DATABASE IF EXISTS {_databaseSettings.ConnectionSettings.Database}");
    }

    [Benchmark]
    public async Task Array()
    {
        await using var connection = _connectionFactory.Create();
        await connection.OpenAsync();

        await using var writer = await connection.CreateColumnWriterAsync(_insertCommand, CancellationToken.None);

        await writer.WriteTableAsync(_objectsArray, RowCount, CancellationToken.None);
    }

    [Benchmark]
    public async Task Memory()
    {
        await using var connection = _connectionFactory.Create();
        await connection.OpenAsync();

        await using var writer = await connection.CreateColumnWriterAsync(_insertCommand, CancellationToken.None);

        await writer.WriteTableAsync(_objectsMemory, RowCount, CancellationToken.None);
    }

    [Benchmark]
    public async Task ReadonlyMemory()
    {
        await using var connection = _connectionFactory.Create();
        await connection.OpenAsync();

        await using var writer = await connection.CreateColumnWriterAsync(_insertCommand, CancellationToken.None);

        await writer.WriteTableAsync(_objectsReadonlyMemory, RowCount, CancellationToken.None);
    }

    public sealed record TestObject(
        long OrderId,
        int[] Items);

    public sealed record TestObjectMemory(
        long OrderId,
        Memory<int> Items);

    public sealed record TestObjectReadonlyMemory(
        long OrderId,
        ReadOnlyMemory<int> Items);

    private async Task CreateTable()
    {
        const string createSql = """
            CREATE TABLE IF NOT EXISTS bench_table
            (
                order_id Int64,
                item Nested (
                    id Int32
                )
            ) ENGINE = MergeTree()
            ORDER BY order_id;
            """;

        await using var connection = _connectionFactory.Create();
        await connection.ExecuteAsync(createSql);
    }
}