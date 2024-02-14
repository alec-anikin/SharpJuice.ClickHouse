using System.Collections;
using AutoFixture;
using Dapper;
using FluentAssertions;
using SharpJuice.Clickhouse.Tests.Infrastructure;

namespace SharpJuice.Clickhouse.Tests;

public sealed class Tests : TestClickHouseStore
{
    private readonly Fixture _fixture;
    private ITableWriter<FlatObject> _writer;

    public Tests()
    {
        _fixture = new Fixture();
        _fixture.Inject(new DateOnly(2022, 9, 14));

        var builder = new TableWriterBuilder(this.GetConnectionFactory());

        _writer = builder.For<FlatObject>("test_table")
            .AddColumn("partner_id", a => a.PartnerId)
            .AddColumn("warehouse_id", a => a.WarehouseId)
            .AddColumn("category_id", a => a.CategoryId ?? -1)
            .AddColumn("item_id", a => a.ItemId)
            .AddColumn("date", a => a.Date)
            .Build();

        Initialize().GetAwaiter().GetResult();
        CreateTable().GetAwaiter().GetResult();
    }

    [Fact]
    public async Task WritingRecords_AsSpan()
    {
        var records = _fixture.CreateMany<FlatObject>(357).ToArray();

        await _writer.Insert(records.AsSpan(), CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEquivalentTo(records, o => o.ComparingByValue<FlatObject>());
    }

    [Fact]
    public async Task WritingRecords_AsArray()
    {
        var records = _fixture.CreateMany<FlatObject>(357).ToArray();

        await _writer.Insert(records, CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEquivalentTo(records, o => o.ComparingByValue<FlatObject>());
    }

    [Theory]
    [MemberData(nameof(GetEnumerables))]
    public async Task WritingRecords_AsEnumerable(IEnumerable<FlatObject> records)
    {
        await _writer.Insert(records, CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEquivalentTo(records, o => o.ComparingByValue<FlatObject>());
    }

    [Fact]
    public async Task WritingEmpty_AsSpan()
    {
        var records = Array.Empty<FlatObject>();

        await _writer.Insert(records.AsSpan(), CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEmpty();
    }

    [Fact]
    public async Task WritingEmpty_AsArray()
    {
        var records = Array.Empty<FlatObject>();

        await _writer.Insert(records, CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEmpty();
    }

    [Fact]
    public async Task WritingEmpty_AsEnumerable()
    {
        var records = Enumerable.Empty<FlatObject>();

        await _writer.Insert(records, CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEmpty();
    }

    public static IEnumerable<object[]> GetEnumerables()
    {
        var fixture = new Fixture();
        fixture.Inject(new DateOnly(2022, 9, 14));

        yield return new object[] { fixture.CreateMany<FlatObject>(357).ToArray() };

        yield return new object[] { fixture.CreateMany<FlatObject>(237).ToList() };

        yield return new object[] { fixture.CreateMany<FlatObject>(135).ToHashSet() };

        yield return new object[] { fixture.CreateMany<FlatObject>(357).Where(x => x.ItemId > 0) };

        yield return new object[] { fixture.CreateMany<FlatObject>(357).ToArray().Select(x => x) };

        yield return new object[] { new ReadOnlyCollection(fixture.CreateMany<FlatObject>(145).ToArray()) };
    }

    private async Task CreateTable()
    {
        const string createSql = @"
            CREATE TABLE IF NOT EXISTS test_table
            (
                partner_id Int64 CODEC(DoubleDelta, ZSTD),
                warehouse_id Int64 CODEC(DoubleDelta, ZSTD),
                category_id Int32 CODEC(DoubleDelta, ZSTD),
                item_id Int64 CODEC(DoubleDelta, ZSTD),
                date Date CODEC(ZSTD),
                timestamp DateTime DEFAULT now() CODEC(ZSTD)
            ) ENGINE = MergeTree()
            ORDER BY partner_id;";

        await using var connection = CreateConnection();
        await connection.ExecuteAsync(createSql);
    }


    private async Task<IEnumerable<FlatObject>> GetClickhouseObjects()
    {
        await using var connection = CreateConnection();
        var objects = await connection.QueryAsync<ClickHouseFlatObject>(@"
            SELECT
                partner_id,
                warehouse_id,
                category_id,
                item_id,
                date
            FROM test_table;");

        return CreateObjects(objects);
    }

    private static IEnumerable<FlatObject> CreateObjects(IEnumerable<ClickHouseFlatObject> source)
    {
        return source.Select(p => new FlatObject(
            PartnerId: p.partner_id,
            WarehouseId: p.warehouse_id,
            CategoryId: p.category_id,
            ItemId: p.item_id,
            Date: p.date));
    }

    private class ReadOnlyCollection : IReadOnlyCollection<FlatObject>
    {
        private readonly IReadOnlyCollection<FlatObject> _items;

        public ReadOnlyCollection(IReadOnlyCollection<FlatObject> items)
            => _items = items;

        public IEnumerator<FlatObject> GetEnumerator()
            => _items.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
            => ((IEnumerable)_items).GetEnumerator();

        public int Count => _items.Count;
    }

    public sealed record FlatObject(
        long PartnerId,
        long WarehouseId,
        int? CategoryId,
        long ItemId,
        DateOnly Date);

    private sealed record ClickHouseFlatObject
    {
        public long partner_id { get; init; }
        public long warehouse_id { get; init; }
        public short category_id { get; init; }
        public long item_id { get; init; }
        public DateOnly date { get; init; }
    }
}