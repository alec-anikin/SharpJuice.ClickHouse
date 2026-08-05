using System.IO.Compression;
using Octonica.ClickHouseClient.Types;
using SharpJuice.Clickhouse.TableSchema;

namespace SharpJuice.Clickhouse.Driver;

internal sealed class DriverTableSinkFactory : ITableSinkFactory
{
    private readonly IClickHouseConnectionFactory _connectionFactory;

    public DriverTableSinkFactory(IClickHouseConnectionFactory connectionFactory)
        => _connectionFactory = connectionFactory;

    public ITableSink Create(string tableName, IReadOnlyList<string> columnNames)
        => new DriverTableSink(_connectionFactory, tableName, columnNames);
}

internal sealed class DriverTableSink : ITableSink
{
    private readonly IClickHouseConnectionFactory _connectionFactory;
    private readonly string _tableName;
    private readonly string _insertCommand;
    private readonly string _schemaQuery;
    private Lazy<Task<Dictionary<string, string>>> _columnTypes;

    public DriverTableSink(
        IClickHouseConnectionFactory connectionFactory,
        string tableName,
        IReadOnlyList<string> columnNames)
    {
        _connectionFactory = connectionFactory;
        _tableName = tableName;

        var columns = string.Join(", ", columnNames);
        _insertCommand = $"INSERT INTO {tableName} ({columns}) FORMAT Native";
        // Zero rows, but the response still carries column names and CH types — the same
        // probe the driver runs internally for its own inserts (it is internal there):
        // https://github.com/ClickHouse/clickhouse-cs/blob/main/ClickHouse.Driver/Utility/SchemaResolver.cs
        _schemaQuery = $"SELECT {columns} FROM {tableName} WHERE 1=0";
        _columnTypes = CreateSchemaCache();
    }

    public async Task Write(ITable table, CancellationToken cancellationToken)
    {
        var schemaCache = _columnTypes;
        try
        {
            var columnTypes = await schemaCache.Value.WaitAsync(cancellationToken);

            var writers = table.Columns
                .SelectMany(c => c.CreateWriters(name => GetTypeInfo(columnTypes, name)))
                .ToArray();

            await using var connection = _connectionFactory.Create();

            // The callback writes straight into the HTTP request body (StreamCallbackContent),
            // so the block is never materialized in memory. isCompressed: true only adds the
            // Content-Encoding: gzip header — compressing is on us, hence the GZipStream:
            // https://github.com/ClickHouse/clickhouse-cs/blob/main/ClickHouse.Driver/ClickHouseClient.cs (PostStreamAsync)
            using var response = await connection.PostStreamAsync(
                _insertCommand,
                async (stream, _) =>
                {
                    // leaveOpen: the request stream is owned by the HttpContent
                    await using var gzip = new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true);
                    NativeBlockWriter.Write(gzip, writers, table.RowCount);
                },
                isCompressed: true,
                cancellationToken);
        }
        catch
        {
            // Never cache a failed probe, and drop a possibly stale schema (e.g. a renamed
            // column after ALTER TABLE) — the next insert re-reads it
            Interlocked.CompareExchange(ref _columnTypes, CreateSchemaCache(), schemaCache);
            throw;
        }
    }

    // Lazy<Task> so that concurrent inserts coalesce into a single schema probe
    private Lazy<Task<Dictionary<string, string>>> CreateSchemaCache()
        => new(ResolveSchema, LazyThreadSafetyMode.ExecutionAndPublication);

    private async Task<Dictionary<string, string>> ResolveSchema()
    {
        await using var connection = _connectionFactory.Create();
        await using var command = connection.CreateCommand(_schemaQuery);
        await using var reader = await command.ExecuteReaderAsync();

        var columnTypes = new Dictionary<string, string>(reader.FieldCount, StringComparer.Ordinal);

        // GetDataTypeName returns the canonical CH type string, e.g. "Decimal(18, 6)":
        // https://github.com/ClickHouse/clickhouse-cs/blob/main/ClickHouse.Driver/ADO/Readers/ClickHouseDataReader.cs
        // It goes both into the Native block header and into Octonica's GetTypeInfo to pick
        // the encoder, so the two cannot diverge.
        for (var i = 0; i < reader.FieldCount; i++)
            columnTypes[reader.GetName(i)] = reader.GetDataTypeName(i);

        return columnTypes;
    }

    private IClickHouseColumnTypeInfo GetTypeInfo(Dictionary<string, string> columnTypes, string columnName)
    {
        if (!columnTypes.TryGetValue(columnName, out var typeName))
            throw new InvalidOperationException($"Column '{columnName}' was not found in table '{_tableName}'.");

        return ClickHouseTypeInfoProvider.Instance.GetTypeInfo(typeName);
    }
}
