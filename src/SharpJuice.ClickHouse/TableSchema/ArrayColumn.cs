using System.Buffers;
using Nerdbank.Streams;

namespace SharpJuice.Clickhouse.TableSchema;

internal sealed class ArrayColumn<TItem, TColumn> : IArrayColumn<TItem>
{
    private readonly Sequence<TColumn> _sequence;
    private readonly Func<TItem, TColumn> _getValue;
    private readonly PooledList<Memory<TColumn>> _values;
    private int _index;
    private readonly string _name;
    private Memory<TColumn> _current;

    public ArrayColumn(string name, int arraysCount, int estimatedArraySize, Func<TItem, TColumn> getValue)
    {
        _sequence = new Sequence<TColumn>(ArrayPool<TColumn>.Shared)
        {
            MinimumSpanLength = arraysCount * estimatedArraySize,
            AutoIncreaseMinimumSpanLength = true
        };

        _getValue = getValue;
        _name = name;
        _values = new PooledList<Memory<TColumn>>(arraysCount);
    }

    public string Name => _name;

    public void StartArray(int length)
    {
        if (length == 0)
        {
            _current = Array.Empty<TColumn>();
            _values.Add(_current);
            _index = 0;
        }
        else
        {
            _current = _sequence.GetMemory(length).Slice(0, length);
            _values.Add(_current);
            _sequence.Advance(length);
            _index = 0;
        }
    }

    public void AddValue(in TItem item)
    {
        var value = _getValue(item);

        _current.Span[_index] = value;
        ++_index;
    }

    public object? GetValues() => _values.Segment;
    
    public void Dispose()
    {
        _sequence.Dispose();
        _values.Dispose();
    }
}