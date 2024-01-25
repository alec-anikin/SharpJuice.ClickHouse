namespace SharpJuice.Clickhouse.TableSchema;

internal sealed class ArrayStatistics
{
    private int _average = 2;

    public int GetAverageSize() => _average == 0 ? 2 : _average;

    public void SetSize(int value)
    {
        if (value == 0) return;

        _average = (value + _average) / 2;
    }
}