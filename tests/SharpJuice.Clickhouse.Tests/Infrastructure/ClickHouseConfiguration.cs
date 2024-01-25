using Octonica.ClickHouseClient;

namespace SharpJuice.Clickhouse.Tests.Infrastructure;

public sealed class ClickHouseConfiguration
{
    private static int _number = 1;

    private const string ClickHouseServerVariableName = "CLICKHOUSE_TEST_SERVER";
    private const string ClickHouseServerPortVariableName = "CLICKHOUSE_TEST_SERVER_PORT";
    private const string ClickHouseUserVariableName = "CLICKHOUSE_USER";
    private const string ClickHousePasswordVariableName = "CLICKHOUSE_PASSWORD";
    private readonly ClickHouseConnectionSettings _connectionSettings;

    public ClickHouseConfiguration()
    {
        var host = Environment.GetEnvironmentVariable(ClickHouseServerVariableName)
                   ?? DefaultSettings.ClickHouseServerHost;

        var portString = Environment.GetEnvironmentVariable(ClickHouseServerPortVariableName);
        var port = portString != null
            ? ushort.Parse(portString)
            : DefaultSettings.ClickHouseServerPort;

        var builder = new ClickHouseConnectionStringBuilder
        {
            Host = host,
            Port = port,
            Database = "chronicles",
            User = Environment.GetEnvironmentVariable(ClickHouseUserVariableName) ?? DefaultSettings.ClickHouseUser,
            Password = Environment.GetEnvironmentVariable(ClickHousePasswordVariableName) ?? DefaultSettings.ClickHousePassword,
            CommandTimeout = 10,
            ReadWriteTimeout = 10000
        };

        _connectionSettings = builder.BuildSettings();
    }

    public ClickHouseDatabaseSettings CreateDatabase(string? name = null)
    {
        if (string.IsNullOrWhiteSpace(name))
            name = "test_" + DateTime.UtcNow.Ticks + "_" + Interlocked.Increment(ref _number);

        return new ClickHouseDatabaseSettings(_connectionSettings, name);
    }
}