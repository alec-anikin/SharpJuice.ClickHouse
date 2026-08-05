using ClickHouse.Driver.ADO;

namespace SharpJuice.Clickhouse.Driver;

public interface IClickHouseConnectionFactory
{
    ClickHouseConnection Create();
}
