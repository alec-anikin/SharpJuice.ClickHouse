using Octonica.ClickHouseClient;

namespace SharpJuice.Clickhouse;

public interface IClickHouseConnectionFactory
{
    ClickHouseConnection Create();
}