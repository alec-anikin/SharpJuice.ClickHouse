using System.Collections;
using AutoFixture;
using Dapper;
using FluentAssertions;
using SharpJuice.Clickhouse.Tests.Infrastructure;

namespace SharpJuice.Clickhouse.Tests;

public sealed class JoinTests : TestClickHouseStore
{
    private readonly Fixture _fixture;
    private ITableWriter<Message> _writer;

    public JoinTests()
    {
        _fixture = new Fixture();
        _fixture.Inject(new DateOnly(2022, 9, 14));

        var builder = new TableWriterBuilder(this.GetConnectionFactory());

        _writer = builder.For<Message>("test_table")
            .AddColumn("partner_id", a => a.PartnerId)
            .AddColumn("warehouse_id", a => a.WarehouseId)
            .ArrayJoin(
                a => a.Items,
                b => b.AddColumn("category_id", a => a.CategoryId ?? -1)
                    .AddColumn("item_id", a => a.ItemId)
                    .AddColumn("price", a => a.Price))
            .AddColumn("date", a => a.Date)
            .Build();

        Initialize().GetAwaiter().GetResult();
        CreateTable().GetAwaiter().GetResult();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(15)]
    public async Task WritingSingleRecord_AsSpan(int itemsCount)
    {
        var record = _fixture.Create<Message>();
        record = record with { Items = _fixture.CreateMany<Item>(itemsCount).ToArray() };

        var records = new[] { record };
        await _writer.Insert(records.AsSpan(), CancellationToken.None);

        var written = (await GetClickhouseMessages()).Single();

        written.Should().BeEquivalentTo(records.Single());
        written.Items.Should().BeEquivalentTo(records.Single().Items);
    }

    [Fact]
    public async Task WritingSingleRecordWithEmptyJoin_Throws()
    {
        var record = _fixture.Create<Message>();
        record = record with { Items = Array.Empty<Item>() };

        var act = () => _writer.Insert(new[] { record }.AsSpan(), CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>();
    }


    [Fact]
    public async Task WritingRecords_AsSpan()
    {
        var records = _fixture.CreateMany<Message>(127).ToArray();

        await _writer.Insert(records.AsSpan(), CancellationToken.None);

        var written = await GetClickhouseMessages();

        written.Should().BeEquivalentTo(records);
    }

    [Theory]
    [MemberData(nameof(GetEnumerables))]
    public async Task WritingRecords_AsEnumerable(IEnumerable<Message> records)
    {
        await _writer.Insert(records, CancellationToken.None);

        var written = await GetClickhouseMessages();

        written.Should().BeEquivalentTo(records);
    }

    public static IEnumerable<object[]> GetEnumerables()
    {
        var fixture = new Fixture();
        fixture.Inject(new DateOnly(2022, 9, 14));

        yield return new object[] { fixture.CreateMany<Message>(157).ToArray() };

        yield return new object[] { fixture.CreateMany<Message>(37).ToList() };

        yield return new object[] { fixture.CreateMany<Message>(95).ToHashSet() };

        yield return new object[] { fixture.CreateMany<Message>(27).Where(x => x.PartnerId > 0) };

        yield return new object[] { fixture.CreateMany<Message>(85).ToArray().Select(x => x) };

        yield return new object[] { new ReadOnlyCollection(fixture.CreateMany<Message>(15).ToArray()) };
    }

    private async Task CreateTable()
    {
        const string createSql = @"
            CREATE TABLE IF NOT EXISTS test_table
            (
                partner_id Int64 CODEC(Delta(8), ZSTD),
                warehouse_id Int64 CODEC(ZSTD),
                category_id Int32 CODEC(ZSTD),
                item_id Int64 CODEC(ZSTD),
                date Date CODEC(ZSTD),
                timestamp DateTime DEFAULT now() CODEC(ZSTD),
                price String
            ) ENGINE = MergeTree()
            ORDER BY partner_id;";

        await using var connection = CreateConnection();
        await connection.ExecuteAsync(createSql);
    }


    private async Task<IEnumerable<Message>> GetClickhouseMessages()
    {
        await using var connection = CreateConnection();
        var messages = await connection.QueryAsync<ClickHouseMessage>(@"
            SELECT
                partner_id,
                warehouse_id,
                category_id,
                item_id,
                price,
                date
            FROM test_table;");

        return CreateMessages(messages);
    }

    private static IEnumerable<Message> CreateMessages(IEnumerable<ClickHouseMessage> source)
    {
        return source
            .GroupBy(x => (seller_id: x.partner_id, x.warehouse_id, x.date))
            .Select(p => new Message(
                PartnerId: p.Key.seller_id,
                WarehouseId: p.Key.warehouse_id,
                Date: p.Key.date,
                p.Select(x => new Item(x.category_id, x.item_id, x.price!)).ToArray()));
    }

    private class ReadOnlyCollection : IReadOnlyCollection<Message>
    {
        private readonly IReadOnlyCollection<Message> _items;

        public ReadOnlyCollection(IReadOnlyCollection<Message> items)
            => _items = items;

        public IEnumerator<Message> GetEnumerator()
            => _items.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
            => ((IEnumerable)_items).GetEnumerator();

        public int Count => _items.Count;
    }

    public sealed record Message(
        long PartnerId,
        long WarehouseId,
        DateOnly Date,
        Item[] Items);

    public sealed record Item(
        int? CategoryId,
        long ItemId,
        string Price);

    private sealed record ClickHouseMessage
    {
        public long partner_id { get; init; }
        public long warehouse_id { get; init; }
        public short category_id { get; init; }
        public long item_id { get; init; }
        public string? price { get; init; }
        public DateOnly date { get; init; }
    }
}
