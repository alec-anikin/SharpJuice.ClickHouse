using System.Buffers;
using System.Runtime.CompilerServices;

namespace SharpJuice.Clickhouse.TableSchema;

internal sealed class Column<TRecord, TColumn> : IColumn<TRecord>
{
    private readonly Func<TRecord, TColumn> _getValue;
    private readonly int _length;
    private TColumn[] _values;
    private int _index;
    private readonly string _name;

    public Column(string name, int recordsCount, Func<TRecord, TColumn> getValue)
    {
        _getValue = getValue;
        _name = name;
        _length = recordsCount;
        _values = ArrayPool<TColumn>.Shared.Rent(recordsCount);
    }

    public void AddValue(in TRecord record, int repeat = 1)
    {
        var value = _getValue(record);

        if (repeat < 2)
            AddValue(value);
        else
        {
            for (var i = repeat; i > 0; --i)
                AddValue(value);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddValue(in TColumn value)
    {
        if (_index >= _length)
            throw new InvalidOperationException("Column capacity exhausted.");

        _values[_index] = value;
        ++_index;
    }

    public IEnumerable<KeyValuePair<string, object?>> GetValues()
    {
        yield return new(_name, new ArraySegment<TColumn>(_values, 0, _length));
    }

    public void Dispose()
    {
        ArrayPool<TColumn>.Shared.Return(_values);
        _values = Array.Empty<TColumn>();
    }
}