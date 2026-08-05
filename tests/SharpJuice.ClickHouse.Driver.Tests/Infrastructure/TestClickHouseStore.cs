using ClickHouse.Driver;
using ClickHouse.Driver.ADO;
using Dapper;
using FluentAssertions;
using Octonica.ClickHouseClient;
using SharpJuice.Clickhouse.Tests.Infrastructure;

namespace SharpJuice.Clickhouse.Driver.Tests.Infrastructure;

public abstract class TestClickHouseStore : IDisposable
{
    private const string ClickHouseServerHttpPortVariableName = "CLICKHOUSE_TEST_SERVER_HTTP_PORT";
    private const ushort DefaultHttpPort = 8123;

    private readonly ClickHouseDatabaseSettings _databaseSettings;

    private readonly Clickhouse.ClickHouseConnectionFactory _readConnectionFactory;

    private readonly ClickHouseConnectionFactory _writeConnectionFactory;

    static TestClickHouseStore()
        => AssertionOptions.AssertEquivalencyUsing(options => options
            .Using<DateTime>(c => c.Subject.Should().BeCloseTo(c.Expectation, TimeSpan.FromMinutes(1)))
            .WhenTypeIs<DateTime>());

    protected TestClickHouseStore()
    {
        var configuration = new ClickHouseConfiguration();
        _databaseSettings = configuration.CreateDatabase();
        _readConnectionFactory = new Clickhouse.ClickHouseConnectionFactory(_databaseSettings.ConnectionSettings);
        _writeConnectionFactory = new ClickHouseConnectionFactory(CreateClientSettings());
    }

    protected async Task Initialize()
    {
        await using var connection = _databaseSettings.CreateConnection(string.Empty);

        await connection.ExecuteAsync(
            $"CREATE DATABASE IF NOT EXISTS {_databaseSettings.ConnectionSettings.Database}");
    }

    protected IClickHouseConnectionFactory GetConnectionFactory()
        => _writeConnectionFactory;

    protected Octonica.ClickHouseClient.ClickHouseConnection CreateConnection()
        => _readConnectionFactory.Create();

    public void Dispose()
    {
        using var connection = _readConnectionFactory.Create();

        connection.ExecuteAsync($"DROP DATABASE IF EXISTS {_databaseSettings.ConnectionSettings.Database}");

        _writeConnectionFactory.Dispose();
    }

    private ClickHouseClientSettings CreateClientSettings()
    {
        var settings = _databaseSettings.ConnectionSettings;

        var portString = Environment.GetEnvironmentVariable(ClickHouseServerHttpPortVariableName);
        var port = portString != null ? ushort.Parse(portString) : DefaultHttpPort;

        return new ClickHouseClientSettings(
            $"Host={settings.Host};Port={port};Database={settings.Database};Username={settings.User};Password={settings.Password}")
        {
            // JSON format settings are not supported by ClickHouse 24.3 used in tests
            JsonReadMode = JsonReadMode.None,
            JsonWriteMode = JsonWriteMode.None
        };
    }
}
