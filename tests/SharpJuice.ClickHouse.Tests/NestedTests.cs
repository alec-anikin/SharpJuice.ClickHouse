using System.Collections;
using System.Diagnostics;
using AutoFixture;
using Dapper;
using FluentAssertions;
using SharpJuice.Clickhouse.Tests.Infrastructure;

namespace SharpJuice.Clickhouse.Tests;

public sealed class NestedTests : TestClickHouseStore
{
    private readonly Fixture _fixture;
    private ITableWriter<TestObject> _writer;

    public NestedTests()
    {
        _fixture = new Fixture();
        _fixture.Inject(new DateOnly(2022, 9, 14));

        var builder = new TableWriterBuilder(this.GetConnectionFactory());

        _writer = builder.For<TestObject>("test_table")
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

        Initialize().GetAwaiter().GetResult();
        CreateTable().GetAwaiter().GetResult();
    }

    [Fact]
    public async Task WritingSingle_AsSpan()
    {
        var obj = _fixture.Create<TestObject>();

        obj = obj with
        {
            Benefits = _fixture.CreateMany<Benefit>(0).ToList(),
            Discounts = new ReadOnlyCollection<Discount>(_fixture.CreateMany<Discount>(1).ToArray()),
            Items = _fixture.CreateMany<Item>(73).ToArray()
        };

        await _writer.Insert(new[] { obj }.AsSpan(), CancellationToken.None);

        var written = (await GetClickhouseObjects()).Single();

        written.Should().BeEquivalentTo(obj, o => o.ComparingByValue<TestObject>());
        written.Items.Should().HaveCount(73);
        written.Items.Should().BeEquivalentTo(obj.Items, c => c.WithStrictOrdering());
        written.Discounts.Should().HaveCount(1);
        written.Discounts.Should().BeEquivalentTo(obj.Discounts, c => c.WithStrictOrdering());
        written.Benefits.Should().HaveCount(0);
        written.Benefits.Should().BeEquivalentTo(obj.Benefits, c => c.WithStrictOrdering());
    }

    [Fact]
    public async Task WritingSingle_Empty()
    {
        var obj = _fixture.Create<TestObject>();

        obj = obj with
        {
            Benefits = new List<Benefit>(),
            Discounts = Array.Empty<Discount>(),
            Items = Array.Empty<Item>()
        };

        await _writer.Insert(new[] { obj }.AsSpan(), CancellationToken.None);

        var written = (await GetClickhouseObjects()).Single();

        written.Should().BeEquivalentTo(obj, o => o.ComparingByValue<TestObject>());
        written.Items.Should().HaveCount(0);
        written.Discounts.Should().HaveCount(0);
        written.Benefits.Should().HaveCount(0);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(16)]
    [InlineData(17)]
    public async Task WritingRecords_AsSpan(int repeat)
    {
        _fixture.RepeatCount = repeat;

        var records = _fixture.CreateMany<TestObject>(58).ToArray();

        await _writer.Insert(records.AsSpan(), CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEquivalentTo(records, o => o.ComparingByValue<TestObject>());
    }

    [Theory]
    [MemberData(nameof(GetEnumerables))]
    public async Task WritingRecords_AsEnumerable(IEnumerable<TestObject> records)
    {
        await _writer.Insert(records, CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEquivalentTo(records, o => o.ComparingByValue<TestObject>());
    }

    public static IEnumerable<object[]> GetEnumerables()
    {
        var fixture = new Fixture();
        fixture.Inject(new DateOnly(2022, 9, 14));

        yield return new object[] { fixture.CreateMany<TestObject>(357).ToArray() };

        yield return new object[] { fixture.CreateMany<TestObject>(237).ToList() };

        yield return new object[] { fixture.CreateMany<TestObject>(135).ToHashSet() };

        yield return new object[] { fixture.CreateMany<TestObject>(357).Where(x => x.OrderId > 0) };

        yield return new object[] { fixture.CreateMany<TestObject>(357).ToArray().Select(x => x) };

        yield return new object[] { new ReadOnlyCollection<TestObject>(fixture.CreateMany<TestObject>(145).ToArray()) };
    }


    private async Task<IEnumerable<TestObject>> GetClickhouseObjects()
    {
        await using var connection = CreateConnection();
        var objects = await connection.QueryAsync<ClickHouseObject>(
            """
            SELECT
            order_id,
            cart_id,
            total_amount,
            date,
            item.id as item_id,
            item.quantity as item_quantity,
            item.name as item_name,
            item.price as item_price,
            discount.id as discount_id,
            discount.name as discount_name,
            discount.value as discount_value,
            benefit.code as benefit_code,
            benefit.value as benefit_value
            FROM test_table
            """);

        return CreateObjects(objects);
    }

    private static IEnumerable<TestObject> CreateObjects(IEnumerable<ClickHouseObject> source)
    {
        return source.Select(p => new TestObject(
            OrderId: p.order_id,
            CartId: p.cart_id,
            TotalAmount: p.total_amount,
            Date: p.date,
            Items: p.item_id.Select((id, index) => new Item(id, p.item_quantity[index], p.item_name[index], p.item_price[index])).ToArray(),
            Discounts: p.discount_id.Select((id, index) => new Discount(id, p.discount_name[index], p.discount_value[index])).ToArray(),
            Benefits: p.benefit_code.Select((code, index) => new Benefit(code, p.benefit_value[index])).ToList()));
    }

    private class ReadOnlyCollection<T> : IReadOnlyCollection<T>
    {
        private readonly IReadOnlyCollection<T> _items;

        public ReadOnlyCollection(IReadOnlyCollection<T> items)
            => _items = items;

        public IEnumerator<T> GetEnumerator()
            => _items.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
            => ((IEnumerable)_items).GetEnumerator();

        public int Count => _items.Count;
    }

    private async Task CreateTable()
    {
        const string createSql = """
            CREATE TABLE IF NOT EXISTS test_table
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

        await using var connection = CreateConnection();
        await connection.ExecuteAsync(createSql);
    }

    public sealed record ClickHouseObject(
        long order_id,
        int cart_id,
        decimal total_amount,
        DateOnly date,
        int[] item_id,
        int[] item_quantity,
        string[] item_name,
        decimal[] item_price,
        int[] discount_id,
        string[] discount_name,
        decimal[] discount_value,
        Guid[] benefit_code,
        int[] benefit_value);


    public sealed record TestObject(
        long OrderId,
        int CartId,
        decimal TotalAmount,
        DateOnly Date,
        Item[] Items,
        IEnumerable<Discount> Discounts,
        List<Benefit> Benefits)
    {
        public bool Equals(TestObject? other)
        {
            if (other == null)
                return false;

            var flatResult = OrderId == other.OrderId &&
                CartId == other.CartId &&
                TotalAmount == other.TotalAmount &&
                Date == other.Date;

            if (flatResult == false)
                return false;

            var items = new HashSet<Item>(Items);
            if (!items.SetEquals(other.Items))
                return false;

            var discounts = new HashSet<Discount>(Discounts);
            if (!discounts.SetEquals(other.Discounts))
                return false;

            var benefits = new HashSet<Benefit>(Benefits);
            return benefits.SetEquals(other.Benefits);
        }

        public override int GetHashCode()
            => HashCode.Combine(OrderId, CartId, TotalAmount, Date, Items.Length, Discounts.Count(), Benefits.Count);
    }

    public readonly record struct Item(int Id, int Quantity, string Name, decimal Price);

    public sealed record Discount(int Id, string Name, decimal Value);

    public sealed record Benefit(Guid Code, int Value);
}