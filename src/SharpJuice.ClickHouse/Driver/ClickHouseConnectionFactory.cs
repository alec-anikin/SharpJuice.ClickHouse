using ClickHouse.Driver;
using ClickHouse.Driver.ADO;
using SharpJuice.Clickhouse.Driver;

namespace SharpJuice.ClickHouse.Driver;

public sealed class ClickHouseConnectionFactory : IClickHouseConnectionFactory, IDisposable
{
    private readonly ClickHouseClient _client;

    public ClickHouseConnectionFactory(ClickHouseClientSettings settings)
        => _client = new ClickHouseClient(settings);

    public ClickHouseConnection Create() => _client.CreateConnection();

    public void Dispose() => _client.Dispose();
}
