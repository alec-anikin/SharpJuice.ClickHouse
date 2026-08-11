using System.Diagnostics.CodeAnalysis;
using AutoFixture;
using Dapper;
using FluentAssertions;
using SharpJuice.Clickhouse.Driver.Tests.Infrastructure;

namespace SharpJuice.Clickhouse.Driver.Tests;

public sealed class CollectionShapesTests : TestClickHouseStore
{
    private readonly Fixture _fixture;
    private readonly ITableWriter<TestObject> _writer;

    public CollectionShapesTests()
    {
        _fixture = new Fixture();
        var builder = new TableWriterBuilder(this.GetConnectionFactory());

        _writer = builder.For<TestObject>("collection_shapes_table")
            .AddColumn("id", a => a.Id)
            .AddNestedColumn("arr", x => x.Array, c => c.AddColumn("value", i => i.Value))
            .AddNestedColumn("lst", x => x.List, c => c.AddColumn("value", i => i.Value))
            .AddNestedColumn("enm", x => x.Enumerable, c => c.AddColumn("value", i => i.Value))
            .AddNestedColumn("mem", x => x.Memory, c => c.AddColumn("value", i => i.Value))
            .Build();

        Initialize().GetAwaiter().GetResult();
        CreateTable().GetAwaiter().GetResult();
    }
    
    [Fact]
    public async Task WritingEmptyCollections()
    {
        var record = CreateRecord(1, []);

        await _writer.Insert(new[] { record }, CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEquivalentTo([Expected(record)]);
    }

    [Fact]
    public async Task WritingRecords()
    {
        var records = _fixture.CreateMany<Item[]>(137)
            .Select((items, id) => CreateRecord(id, items))
            .ToArray();

        await _writer.Insert(records, CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEquivalentTo(records.Select(Expected));
    }

    private static TestObject CreateRecord(int id, Item[] items)
        => new(
            Id: id,
            Array: items,
            List: items.ToList(),
            Enumerable: items.Select(x => x),
            Memory: items.AsMemory());

    private static ClickHouseObject Expected(TestObject record)
    {
        var values = record.Array.Select(x => x.Value).ToArray();

        return new ClickHouseObject(record.Id, values, values, values, values);
    }

    private async Task<IEnumerable<ClickHouseObject>> GetClickhouseObjects()
    {
        await using var connection = CreateConnection();

        return await connection.QueryAsync<ClickHouseObject>("""
            SELECT
                id,
                arr.value as arr_value,
                lst.value as lst_value,
                enm.value as enm_value,
                mem.value as mem_value
            FROM collection_shapes_table
            ORDER BY id
            """);
    }

    private async Task CreateTable()
    {
        const string createSql = """
            CREATE TABLE IF NOT EXISTS collection_shapes_table
            (
                id Int32,
                arr Nested (value Int32),
                lst Nested (value Int32),
                enm Nested (value Int32),
                mem Nested (value Int32)
            ) ENGINE = MergeTree()
            ORDER BY id;
            """;

        await using var connection = CreateConnection();
        await connection.ExecuteAsync(createSql);
    }

    private readonly record struct Item(int Value);

    private sealed record TestObject(
        int Id,
        Item[] Array,
        List<Item> List,
        IEnumerable<Item> Enumerable,
        ReadOnlyMemory<Item> Memory);

    [SuppressMessage("ReSharper", "InconsistentNaming")]
    private sealed record ClickHouseObject(
        int id,
        int[] arr_value,
        int[] lst_value,
        int[] enm_value,
        int[] mem_value);
}
