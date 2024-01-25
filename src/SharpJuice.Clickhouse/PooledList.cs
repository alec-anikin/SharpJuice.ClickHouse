using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace SharpJuice.Clickhouse;

[DebuggerDisplay("Count = {Count}")]
internal class PooledList<T> : IDisposable
{
    private const int MaxArrayLength = 0x7FEFFFFF;
    private const int DefaultCapacity = 4;
    private const int PoolingThreshold = 128;

    private T[] _items;
    private int _size;
    private readonly bool _clearOnFree;

    public PooledList(int capacity) : this(capacity, ClearMode.Auto)
    {
    }

    public PooledList(int capacity, ClearMode clearMode)
    {
        if (capacity < 0)
            throw new ArgumentException("Capacity must be greater than 0", nameof(capacity));

        _clearOnFree = ShouldClear(clearMode);

        _items = Rent(capacity);
    }

    public int Capacity
    {
        get => _items.Length;
        set
        {
            if (value < _size)
                throw new ArgumentException("Capacity must be greater than current count", nameof(value));

            if (value == _items.Length)
                return;

            if (value > 0)
            {
                var newItems = Rent(value);
                if (_size > 0)
                {
                    Array.Copy(_items, newItems, _size);
                }

                ReturnArray();
                _items = newItems;
            }
            else
            {
                ReturnArray();
                _size = 0;
            }
        }
    }

    public T this[int index]
    {
        get
        {
            if ((uint)index >= (uint)_size)
                throw new ArgumentOutOfRangeException(nameof(index));

            return _items[index];
        }
    }

    public ReadOnlySpan<T> Span => _items.AsSpan(0, _size);

    public ReadOnlyMemory<T> Memory => _items.AsMemory(0, _size);

    public ArraySegment<T> Segment => new(_items, 0, _size);

    public int Count => _items != null ? _size : throw new ObjectDisposedException(nameof(PooledList<T>));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(T item)
    {
        var size = _size;
        if ((uint)size < (uint)_items.Length)
        {
            _size = size + 1;
            _items[size] = item;
        }
        else
        {
            AddWithResize(item);
        }
    }

    public void Dispose()
    {
        ReturnArray();
        _size = 0;
        _items = Array.Empty<T>();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void AddWithResize(T item)
    {
        var size = _size;
        EnsureCapacity(size + 1);
        _size = size + 1;
        _items[size] = item;
    }

    private T[] Rent(int capacity)
    {
        return capacity == 0
            ? Array.Empty<T>()
            : capacity < PoolingThreshold
                ? new T[capacity]
                : ArrayPool<T>.Shared.Rent(capacity);
    }

    private void EnsureCapacity(int min)
    {
        if (_items.Length < min)
        {
            var newCapacity = _items.Length == 0 ? DefaultCapacity : _items.Length * 2;
            if ((uint)newCapacity > MaxArrayLength) newCapacity = MaxArrayLength;
            if (newCapacity < min) newCapacity = min;
            Capacity = newCapacity;
        }
    }

    private bool IsPooled()
        => _items.Length >= PoolingThreshold;

    private void ReturnArray()
    {
        if (_items.Length == 0)
            return;

        if (IsPooled())
        {
            try
            {
                ArrayPool<T>.Shared.Return(_items, clearArray: _clearOnFree);
            }
            catch (ArgumentException)
            {
            }
        }

        _items = Array.Empty<T>();
    }

    private static bool ShouldClear(ClearMode mode)
    {
        return mode == ClearMode.Always ||
               (mode == ClearMode.Auto &&
                RuntimeHelpers.IsReferenceOrContainsReferences<T>());
    }

    public enum ClearMode
    {
        Auto = 0,
        Always = 1,
        Never = 2
    }
}