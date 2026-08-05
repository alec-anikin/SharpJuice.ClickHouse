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
            // Drop possibly stale schema
            Interlocked.CompareExchange(ref _columnTypes, CreateSchemaCache(), schemaCache);
            throw;
        }
    }

    private Lazy<Task<Dictionary<string, string>>> CreateSchemaCache()
        => new(ResolveSchema, LazyThreadSafetyMode.ExecutionAndPublication);

    private async Task<Dictionary<string, string>> ResolveSchema()
    {
        await using var connection = _connectionFactory.Create();
        await using var command = connection.CreateCommand(_schemaQuery);
        await using var reader = await command.ExecuteReaderAsync();

        var columnTypes = new Dictionary<string, string>(reader.FieldCount, StringComparer.Ordinal);
        
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
