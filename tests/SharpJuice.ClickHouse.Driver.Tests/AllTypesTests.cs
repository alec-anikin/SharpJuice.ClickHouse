using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Numerics;
using AutoFixture;
using Dapper;
using FluentAssertions;
using SharpJuice.Clickhouse.Driver.Tests.Infrastructure;

namespace SharpJuice.Clickhouse.Driver.Tests;

public sealed class AllTypesTests : TestClickHouseStore
{
    private readonly Fixture _fixture;
    private ITableWriter<ClickHouseAllTypes> _writer;

    public AllTypesTests()
    {
        _fixture = new Fixture();
        _fixture.Inject(new DateOnly(2022, 9, 14));
        _fixture.Register(() => IPAddress.Parse("10.0.0.1"));

        var builder = new TableWriterBuilder(this.GetConnectionFactory());

        _writer = builder.For<ClickHouseAllTypes>("all_types_table")
            .AddColumn("int8", a => a.int8)
            .AddColumn("int16", a => a.int16)
            .AddColumn("int32", a => a.int32)
            .AddColumn("int64", a => a.int64)
            .AddColumn("int128", a => a.int128)
            .AddColumn("uint8", a => a.uint8)
            .AddColumn("uint16", a => a.uint16)
            .AddColumn("uint32", a => a.uint32)
            .AddColumn("uint64", a => a.uint64)
            .AddColumn("float32", a => a.float32)
            .AddColumn("float64", a => a.float64)
            .AddColumn("decimal_v", a => a.decimal_v)
            .AddColumn("decimal128", a => a.decimal128)
            .AddColumn("bool_v", a => a.bool_v)
            .AddColumn("string_v", a => a.string_v)
            .AddColumn("fixed_string", a => a.fixed_string)
            .AddColumn("date", a => a.date)
            .AddColumn("date32", a => a.date32)
            .AddColumn("datetime", a => a.datetime)
            .AddColumn("datetime64", a => a.datetime64)
            .AddColumn("uuid", a => a.uuid)
            .AddColumn("ipv4", a => a.ipv4)
            .AddColumn("ipv6", a => a.ipv6)
            .AddColumn("enum8", a => a.enum8)
            .AddColumn("enum16", a => a.enum16)
            .AddColumn("nullable_v", a => a.nullable_v)
            .AddColumn("low_cardinality", a => a.low_cardinality)
            .AddColumn("array_int64", a => a.array_int64)
            .AddColumn("array_string", a => a.array_string)
            .AddColumn("array_nullable", a => a.array_nullable)
            .AddColumn("array_array", a => a.array_array)
            .Build();

        Initialize().GetAwaiter().GetResult();
        CreateTable().GetAwaiter().GetResult();
    }
    
    [Fact]
    public async Task Test()
    {
        var records = CreateRecords(67);

        await _writer.Insert(records, CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEquivalentTo(records);
    }
    
    private ClickHouseAllTypes[] CreateRecords(int count)
        => _fixture.CreateMany<ClickHouseAllTypes>(count)
            .Select((x, i) => x with
            {
                int128 = BigInteger.Parse("-170141183460469231731687303715884105728") + i,
                decimal_v = Math.Round(x.decimal_v, 6),
                decimal128 = Math.Round(x.decimal128, 10),
                fixed_string = "abcd"u8.ToArray(),
                date32 = new DateOnly(1960, 3, 4),
                datetime = new DateTimeOffset(2022, 9, 14, 10, 20, 30, TimeSpan.Zero),
                datetime64 = new DateTimeOffset(2022, 9, 14, 10, 20, 30, 123, TimeSpan.Zero),
                ipv4 = IPAddress.Parse("10.0.0." + i % 256),
                ipv6 = IPAddress.Parse("2001:db8::" + i % 256),
                enum8 = i % 2 == 0 ? "a" : "b",
                enum16 = i % 2 == 0 ? "x" : "y",
                nullable_v = i % 3 == 0 ? null : x.nullable_v,
                array_nullable = [i, null],
                array_array = [[i, -i], []]
            })
            .ToArray();

    private async Task<IEnumerable<ClickHouseAllTypes>> GetClickhouseObjects()
    {
        await using var connection = CreateConnection();

        return await connection.QueryAsync<ClickHouseAllTypes>("""
            SELECT
                int8, int16, int32, int64, int128,
                uint8, uint16, uint32, uint64,
                float32, float64, decimal_v, decimal128,
                bool_v, string_v, fixed_string,
                date, date32, datetime, datetime64,
                uuid, ipv4, ipv6, enum8, enum16,
                nullable_v, low_cardinality,
                array_int64, array_string, array_nullable, array_array
            FROM all_types_table
            ORDER BY int128
            """);
    }

    private async Task CreateTable()
    {
        const string createSql = """
            CREATE TABLE IF NOT EXISTS all_types_table
            (
                int8 Int8,
                int16 Int16,
                int32 Int32,
                int64 Int64,
                int128 Int128,
                uint8 UInt8,
                uint16 UInt16,
                uint32 UInt32,
                uint64 UInt64,
                float32 Float32,
                float64 Float64,
                decimal_v Decimal(18, 6),
                decimal128 Decimal(38, 10),
                bool_v Bool,
                string_v String,
                fixed_string FixedString(4),
                date Date,
                date32 Date32,
                datetime DateTime('UTC'),
                datetime64 DateTime64(3, 'UTC'),
                uuid UUID,
                ipv4 IPv4,
                ipv6 IPv6,
                enum8 Enum8('a' = 1, 'b' = 2),
                enum16 Enum16('x' = 100, 'y' = 200),
                nullable_v Nullable(Int32),
                low_cardinality LowCardinality(String),
                array_int64 Array(Int64),
                array_string Array(String),
                array_nullable Array(Nullable(Int32)),
                array_array Array(Array(Int64))
            ) ENGINE = MergeTree()
            ORDER BY int128;
            """;

        await using var connection = CreateConnection();
        await connection.ExecuteAsync(createSql);
    }
    
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    private sealed record ClickHouseAllTypes(
        sbyte int8,
        short int16,
        int int32,
        long int64,
        BigInteger int128,
        byte uint8,
        ushort uint16,
        uint uint32,
        ulong uint64,
        float float32,
        double float64,
        decimal decimal_v,
        decimal decimal128,
        bool bool_v,
        string string_v,
        byte[] fixed_string,
        DateOnly date,
        DateOnly date32,
        DateTimeOffset datetime,
        DateTimeOffset datetime64,
        Guid uuid,
        IPAddress ipv4,
        IPAddress ipv6,
        string enum8,
        string enum16,
        int? nullable_v,
        string low_cardinality,
        long[] array_int64,
        string[] array_string,
        int?[] array_nullable,
        long[][] array_array);
}
