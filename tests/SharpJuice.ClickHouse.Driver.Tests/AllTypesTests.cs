using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Numerics;
using System.Text;
using AutoFixture;
using Dapper;
using FluentAssertions;
using SharpJuice.Clickhouse.Driver.Tests.Infrastructure;

namespace SharpJuice.Clickhouse.Driver.Tests;

public sealed class AllTypesTests : TestClickHouseStore
{
    private readonly Fixture _fixture;
    private readonly ITableWriter<AllTypesObject> _writer;

    public AllTypesTests()
    {
        _fixture = new Fixture();
        _fixture.Inject(new DateOnly(2022, 9, 14));
        _fixture.Register(() => IPAddress.Parse("10.0.0.1"));

        var builder = new TableWriterBuilder(this.GetConnectionFactory());

        _writer = builder.For<AllTypesObject>("all_types_table")
            .AddColumn("int8", a => a.Int8)
            .AddColumn("int16", a => a.Int16)
            .AddColumn("int32", a => a.Int32)
            .AddColumn("int64", a => a.Int64)
            .AddColumn("int128", a => a.Int128)
            .AddColumn("uint8", a => a.UInt8)
            .AddColumn("uint16", a => a.UInt16)
            .AddColumn("uint32", a => a.UInt32)
            .AddColumn("uint64", a => a.UInt64)
            .AddColumn("float32", a => a.Float32)
            .AddColumn("float64", a => a.Float64)
            .AddColumn("decimal_v", a => a.Decimal)
            .AddColumn("decimal128", a => a.Decimal128)
            .AddColumn("bool_v", a => a.Bool)
            .AddColumn("string_v", a => a.String)
            .AddColumn("string_bytes", a => a.StringBytes)
            .AddColumn("string_chars", a => a.StringChars)
            .AddColumn("string_memory", a => a.StringMemory)
            .AddColumn("fixed_string", a => a.FixedString)
            .AddColumn("fixed_string_bytes", a => a.FixedStringBytes)
            .AddColumn("date", a => a.Date)
            .AddColumn("date32", a => a.Date32)
            .AddColumn("date_from_datetime", a => a.DateFromDateTime)
            .AddColumn("datetime", a => a.DateTime)
            .AddColumn("datetime_offset", a => a.DateTimeOffset)
            .AddColumn("datetime64", a => a.DateTime64)
            .AddColumn("datetime64_offset", a => a.DateTime64Offset)
            .AddColumn("uuid", a => a.Uuid)
            .AddColumn("ipv4", a => a.IPv4)
            .AddColumn("ipv4_string", a => a.IPv4String)
            .AddColumn("ipv6", a => a.IPv6)
            .AddColumn("ipv6_string", a => a.IPv6String)
            .AddColumn("enum8", a => a.Enum8)
            .AddColumn("enum16", a => a.Enum16)
            .AddColumn("nullable_v", a => a.Nullable)
            .AddColumn("low_cardinality", a => a.LowCardinality)
            .AddColumn("array_int64", a => a.ArrayInt64)
            .AddColumn("array_string", a => a.ArrayString)
            .AddColumn("array_nullable", a => a.ArrayNullable)
            .AddColumn("array_array", a => a.ArrayArray)
            .AddColumn("map_v", a => a.Map)
            .AddColumn("map_pairs", a => a.MapPairs)
            .AddColumn("tuple_v", a => a.Tuple)
            .AddColumn("tuple_class", a => a.TupleClass)
            .Build();

        Initialize().GetAwaiter().GetResult();
        CreateTable().GetAwaiter().GetResult();
    }

    [Fact]
    public async Task WritingRecords()
    {
        var records = CreateRecords(67);

        await _writer.Insert(records, CancellationToken.None);

        var written = await GetClickhouseObjects();

        written.Should().BeEquivalentTo(records, o => o
            .Using<ReadOnlyMemory<char>>(c => c.Subject.ToString().Should().Be(c.Expectation.ToString()))
            .WhenTypeIs<ReadOnlyMemory<char>>());
    }

    private AllTypesObject[] CreateRecords(int count)
    {
        var moment = new DateTime(2022, 9, 14, 10, 20, 30, DateTimeKind.Utc);

        return _fixture.CreateMany<AllTypesObject>(count)
            .Select((x, i) => x with
            {
                FixedString = "abcd",
                FixedStringBytes = "wxyz"u8.ToArray(),
                StringBytes = Encoding.UTF8.GetBytes("bytes " + i),
                DateFromDateTime = new DateTime(2001, 2, 3, 0, 0, 0, DateTimeKind.Utc),
                DateTime = moment,
                DateTimeOffset = new DateTimeOffset(moment),
                DateTime64 = moment.AddMilliseconds(123),
                DateTime64Offset = new DateTimeOffset(moment.AddMilliseconds(123)),
                IPv6 = IPAddress.Parse("2001:db8::" + i % 256),
                IPv4String = "192.168.1." + i % 256,
                IPv6String = IPAddress.Parse("fe80::" + i % 256).ToString(),
                Enum8 = i % 2 == 0 ? "a" : "b",
                Enum16 = i % 2 == 0 ? "x" : "y"
            })
            .ToArray();
    }

    private async Task<IEnumerable<AllTypesObject>> GetClickhouseObjects()
    {
        await using var connection = CreateConnection();

        var objects = await connection.QueryAsync<ClickHouseAllTypesObject>("""
            SELECT
                int8, int16, int32, int64, int128,
                uint8, uint16, uint32, uint64,
                float32, float64, decimal_v, decimal128,
                bool_v, string_v, string_bytes, string_chars, string_memory,
                fixed_string, fixed_string_bytes,
                date, date32, date_from_datetime,
                datetime, datetime_offset, datetime64, datetime64_offset,
                uuid, ipv4, ipv4_string, ipv6, ipv6_string, enum8, enum16,
                nullable_v, low_cardinality,
                array_int64, array_string, array_nullable, array_array,
                map_v, map_pairs, tuple_v, tuple_class
            FROM all_types_table
            ORDER BY int128
            """);

        return objects.Select(x => x.ToAllTypes());
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
                string_bytes String,
                string_chars String,
                string_memory String,
                fixed_string FixedString(4),
                fixed_string_bytes FixedString(4),
                date Date,
                date32 Date32,
                date_from_datetime Date,
                datetime DateTime('UTC'),
                datetime_offset DateTime('UTC'),
                datetime64 DateTime64(3, 'UTC'),
                datetime64_offset DateTime64(3, 'UTC'),
                uuid UUID,
                ipv4 IPv4,
                ipv4_string IPv4,
                ipv6 IPv6,
                ipv6_string IPv6,
                enum8 Enum8('a' = 1, 'b' = 2),
                enum16 Enum16('x' = 100, 'y' = 200),
                nullable_v Nullable(Int32),
                low_cardinality LowCardinality(String),
                array_int64 Array(Int64),
                array_string Array(String),
                array_nullable Array(Nullable(Int32)),
                array_array Array(Array(Int64)),
                map_v Map(String, Int64),
                map_pairs Map(String, Int64),
                tuple_v Tuple(Int64, String),
                tuple_class Tuple(Int64, String)
            ) ENGINE = MergeTree()
            ORDER BY int128;
            """;

        await using var connection = CreateConnection();
        await connection.ExecuteAsync(createSql);
    }

    private sealed record AllTypesObject(
        sbyte Int8,
        short Int16,
        int Int32,
        long Int64,
        BigInteger Int128,
        byte UInt8,
        ushort UInt16,
        uint UInt32,
        ulong UInt64,
        float Float32,
        double Float64,
        decimal Decimal,
        decimal Decimal128,
        bool Bool,
        string String,
        byte[] StringBytes,
        char[] StringChars,
        ReadOnlyMemory<char> StringMemory,
        string FixedString,
        byte[] FixedStringBytes,
        DateOnly Date,
        DateOnly Date32,
        DateTime DateFromDateTime,
        DateTime DateTime,
        DateTimeOffset DateTimeOffset,
        DateTime DateTime64,
        DateTimeOffset DateTime64Offset,
        Guid Uuid,
        IPAddress IPv4,
        string IPv4String,
        IPAddress IPv6,
        string IPv6String,
        string Enum8,
        string Enum16,
        int? Nullable,
        string LowCardinality,
        long[] ArrayInt64,
        string[] ArrayString,
        int?[] ArrayNullable,
        long[][] ArrayArray,
        Dictionary<string, long> Map,
        KeyValuePair<string, long>[] MapPairs,
        (long, string) Tuple,
        Tuple<long, string> TupleClass);

    [SuppressMessage("ReSharper", "InconsistentNaming")]
    private sealed record ClickHouseAllTypesObject(
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
        string string_bytes,
        string string_chars,
        string string_memory,
        byte[] fixed_string,
        byte[] fixed_string_bytes,
        DateOnly date,
        DateOnly date32,
        DateOnly date_from_datetime,
        DateTimeOffset datetime,
        DateTimeOffset datetime_offset,
        DateTimeOffset datetime64,
        DateTimeOffset datetime64_offset,
        Guid uuid,
        IPAddress ipv4,
        IPAddress ipv4_string,
        IPAddress ipv6,
        IPAddress ipv6_string,
        string enum8,
        string enum16,
        int? nullable_v,
        string low_cardinality,
        long[] array_int64,
        string[] array_string,
        int?[] array_nullable,
        long[][] array_array,
        KeyValuePair<string, long>[] map_v,
        KeyValuePair<string, long>[] map_pairs,
        Tuple<long, string> tuple_v,
        Tuple<long, string> tuple_class)
    {
        public AllTypesObject ToAllTypes()
            => new(
                Int8: int8,
                Int16: int16,
                Int32: int32,
                Int64: int64,
                Int128: int128,
                UInt8: uint8,
                UInt16: uint16,
                UInt32: uint32,
                UInt64: uint64,
                Float32: float32,
                Float64: float64,
                Decimal: decimal_v,
                Decimal128: decimal128,
                Bool: bool_v,
                String: string_v,
                StringBytes: Encoding.UTF8.GetBytes(string_bytes),
                StringChars: string_chars.ToCharArray(),
                StringMemory: string_memory.AsMemory(),
                FixedString: Encoding.UTF8.GetString(fixed_string),
                FixedStringBytes: fixed_string_bytes,
                Date: date,
                Date32: date32,
                DateFromDateTime: date_from_datetime.ToDateTime(TimeOnly.MinValue),
                DateTime: datetime.UtcDateTime,
                DateTimeOffset: datetime_offset,
                DateTime64: datetime64.UtcDateTime,
                DateTime64Offset: datetime64_offset,
                Uuid: uuid,
                IPv4: ipv4,
                IPv4String: ipv4_string.ToString(),
                IPv6: ipv6,
                IPv6String: ipv6_string.ToString(),
                Enum8: enum8,
                Enum16: enum16,
                Nullable: nullable_v,
                LowCardinality: low_cardinality,
                ArrayInt64: array_int64,
                ArrayString: array_string,
                ArrayNullable: array_nullable,
                ArrayArray: array_array,
                Map: map_v.ToDictionary(x => x.Key, x => x.Value),
                MapPairs: map_pairs,
                Tuple: (tuple_v.Item1, tuple_v.Item2),
                TupleClass: tuple_class);
    }
}
