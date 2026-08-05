using Dapper;
using FluentAssertions;
using SharpJuice.Clickhouse.Driver.Tests.Infrastructure;

namespace SharpJuice.Clickhouse.Driver.Tests;

public sealed class TypeTests : TestClickHouseStore
{
    private readonly ITableWriter<TypeObject> _writer;

    public TypeTests()
    {
        var builder = new TableWriterBuilder(this.GetConnectionFactory());

        _writer = builder.For<TypeObject>("type_table")
            .AddColumn("id", a => a.Id)
            .AddColumn("v_int64", a => a.Int64Value)
            .AddColumn("v_int32", a => a.Int32Value)
            .AddColumn("v_decimal", a => a.DecimalValue)
            .AddColumn("v_string", a => a.StringValue)
            .AddColumn("v_date", a => a.DateValue)
            .AddColumn("v_uuid", a => a.UuidValue)
            .AddColumn("a_int64", a => a.Int64Array)
            .AddColumn("a_string", a => a.StringArray)
            .AddColumn("a_uuid", a => a.UuidArray)
            .Build();

        Initialize().GetAwaiter().GetResult();
        CreateTable().GetAwaiter().GetResult();
    }

    [Fact]
    public async Task WritingBoundaryValues()
    {
        var records = new[]
        {
            new TypeObject(
                Id: 1,
                Int64Value: long.MinValue,
                Int32Value: int.MinValue,
                DecimalValue: -999_999_999_999.999999m,
                StringValue: string.Empty,
                DateValue: new DateOnly(1970, 1, 2),
                UuidValue: Guid.Empty,
                Int64Array: Array.Empty<long>(),
                StringArray: Array.Empty<string>(),
                UuidArray: Array.Empty<Guid>()),
            new TypeObject(
                Id: 2,
                Int64Value: long.MaxValue,
                Int32Value: int.MaxValue,
                DecimalValue: 999_999_999_999.999999m,
                StringValue: "тест 🚀 ünïcode",
                DateValue: new DateOnly(2149, 6, 6),
                UuidValue: Guid.NewGuid(),
                Int64Array: new[] { long.MinValue, 0L, long.MaxValue },
                StringArray: new[] { string.Empty, "b", "тест" },
                UuidArray: new[] { Guid.Empty, Guid.NewGuid() })
        };

        await _writer.Insert(records, CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEquivalentTo(records);
    }

    [Fact]
    public async Task WritingStringLargerThanWriteBuffer()
    {
        var records = new[]
        {
            new TypeObject(
                Id: 1,
                Int64Value: 1,
                Int32Value: 1,
                DecimalValue: 1m,
                StringValue: new string('щ', 200_000),
                DateValue: new DateOnly(2022, 9, 14),
                UuidValue: Guid.NewGuid(),
                Int64Array: new[] { 1L },
                StringArray: new[] { new string('x', 150_000) },
                UuidArray: Array.Empty<Guid>())
        };

        await _writer.Insert(records, CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEquivalentTo(records);
    }

    private async Task CreateTable()
    {
        const string createSql = """
            CREATE TABLE IF NOT EXISTS type_table
            (
                id Int32,
                v_int64 Int64,
                v_int32 Int32,
                v_decimal Decimal(18, 6),
                v_string String,
                v_date Date,
                v_uuid UUID,
                a_int64 Array(Int64),
                a_string Array(String),
                a_uuid Array(UUID)
            ) ENGINE = MergeTree()
            ORDER BY id;
            """;

        await using var connection = CreateConnection();
        await connection.ExecuteAsync(createSql);
    }

    private async Task<IEnumerable<TypeObject>> GetClickhouseObjects()
    {
        await using var connection = CreateConnection();
        var objects = await connection.QueryAsync<ClickHouseTypeObject>("""
            SELECT
                id,
                v_int64,
                v_int32,
                v_decimal,
                v_string,
                v_date,
                v_uuid,
                a_int64,
                a_string,
                a_uuid
            FROM type_table
            ORDER BY id;
            """);

        return objects.Select(p => new TypeObject(
            Id: p.id,
            Int64Value: p.v_int64,
            Int32Value: p.v_int32,
            DecimalValue: p.v_decimal,
            StringValue: p.v_string!,
            DateValue: p.v_date,
            UuidValue: p.v_uuid,
            Int64Array: p.a_int64!,
            StringArray: p.a_string!,
            UuidArray: p.a_uuid!));
    }

    public sealed record TypeObject(
        int Id,
        long Int64Value,
        int Int32Value,
        decimal DecimalValue,
        string StringValue,
        DateOnly DateValue,
        Guid UuidValue,
        long[] Int64Array,
        string[] StringArray,
        Guid[] UuidArray);

    private sealed record ClickHouseTypeObject
    {
        public int id { get; init; }
        public long v_int64 { get; init; }
        public int v_int32 { get; init; }
        public decimal v_decimal { get; init; }
        public string? v_string { get; init; }
        public DateOnly v_date { get; init; }
        public Guid v_uuid { get; init; }
        public long[]? a_int64 { get; init; }
        public string[]? a_string { get; init; }
        public Guid[]? a_uuid { get; init; }
    }
}
