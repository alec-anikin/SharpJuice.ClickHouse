using ClickHouse.Driver;
using ClickHouse.Driver.ADO;

namespace SharpJuice.Clickhouse.Driver;

public sealed class ClickHouseConnectionFactory : IClickHouseConnectionFactory, IDisposable
{
    private readonly ClickHouseClient _client;

    public ClickHouseConnectionFactory(ClickHouseClientSettings settings)
        => _client = new ClickHouseClient(settings);

    public ClickHouseConnection Create() => _client.CreateConnection();

    public void Dispose() => _client.Dispose();
}
