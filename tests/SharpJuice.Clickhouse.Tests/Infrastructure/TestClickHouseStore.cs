using Dapper;
using FluentAssertions;
using Octonica.ClickHouseClient;

namespace SharpJuice.Clickhouse.Tests.Infrastructure;

public abstract class TestClickHouseStore : IDisposable
{
    private readonly ClickHouseDatabaseSettings _databaseSettings;

    private readonly ClickHouseConnectionFactory _connectionFactory;

    static TestClickHouseStore()
        => AssertionOptions.AssertEquivalencyUsing(options => options
            .Using<DateTime>(c => c.Subject.Should().BeCloseTo(c.Expectation, TimeSpan.FromMinutes(1)))
            .WhenTypeIs<DateTime>());

    protected TestClickHouseStore()
    {
        var configuration = new ClickHouseConfiguration();
        _databaseSettings = configuration.CreateDatabase();
        _connectionFactory = new ClickHouseConnectionFactory(_databaseSettings.ConnectionSettings);
    }

    protected async Task Initialize()
    {
        await using var connection = _databaseSettings.CreateConnection(string.Empty);

        await connection.ExecuteAsync(
            $"CREATE DATABASE IF NOT EXISTS {_databaseSettings.ConnectionSettings.Database}");
    }

    protected IClickHouseConnectionFactory GetConnectionFactory()
        => _connectionFactory;

    protected ClickHouseConnection CreateConnection()
        => _connectionFactory.Create();

    public void Dispose()
    {
        using var connection = _connectionFactory.Create();

        connection.ExecuteAsync($"DROP DATABASE IF EXISTS {_databaseSettings.ConnectionSettings.Database}");
    }
}