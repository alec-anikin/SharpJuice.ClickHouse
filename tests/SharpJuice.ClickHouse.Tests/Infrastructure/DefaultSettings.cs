namespace SharpJuice.Clickhouse.Tests.Infrastructure;

public static class DefaultSettings
{
    public static string ClickHouseServerHost { get; } = "localhost";

    public static ushort ClickHouseServerPort { get; } = 9000;

    public static string ClickHouseUser { get; } = "default";

    public static string ClickHousePassword { get; } = string.Empty;
}