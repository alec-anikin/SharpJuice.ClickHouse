using Octonica.ClickHouseClient;

namespace SharpJuice.Clickhouse.Tests.Infrastructure;

public sealed class ClickHouseDatabaseSettings 
{
    public ClickHouseConnectionSettings ConnectionSettings { get; }

    public ClickHouseDatabaseSettings(ClickHouseConnectionSettings connectionSettings, string newDbName)
    {
        if (connectionSettings == null)
            throw new ArgumentNullException(nameof(connectionSettings));

        if (string.IsNullOrWhiteSpace(newDbName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(newDbName));

        var builder = new ClickHouseConnectionStringBuilder(connectionSettings)
        {
            Database = newDbName
        };

        ConnectionSettings = builder.BuildSettings();
    }

    public ClickHouseConnection CreateConnection(string database)
    {
        var builder = new ClickHouseConnectionStringBuilder(ConnectionSettings)
        {
            Database = database
        };

        return new(builder.BuildSettings());
    }
}