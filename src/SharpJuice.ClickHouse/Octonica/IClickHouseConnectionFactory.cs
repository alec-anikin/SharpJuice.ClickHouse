
using Octonica.ClickHouseClient;

namespace SharpJuice.Clickhouse.Octonica;

public interface IClickHouseConnectionFactory
{
    ClickHouseConnection Create();
}
