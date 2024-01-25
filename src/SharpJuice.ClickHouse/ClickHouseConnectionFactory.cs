using Octonica.ClickHouseClient;

namespace SharpJuice.Clickhouse;

public sealed class ClickHouseConnectionFactory : IClickHouseConnectionFactory
{
    private readonly ClickHouseConnectionSettings _connectionSettings;

    public ClickHouseConnectionFactory(ClickHouseConnectionSettings connectionSettings)
        => _connectionSettings = connectionSettings;

    public ClickHouseConnection Create() => new(_connectionSettings);
}