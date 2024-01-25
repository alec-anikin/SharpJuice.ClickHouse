using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace SharpJuice.Clickhouse;

internal sealed class CollectionAccessor<TRecord, TItem>
{
    private readonly Func<TRecord, List<TItem>>? _getItemsList;
    private readonly Func<TRecord, TItem[]>? _getItemsArray;
    private readonly Func<TRecord, IEnumerable<TItem>>? _getItemsCollection;
    private readonly Func<TRecord, ReadOnlyMemory<TItem>>? _getItemsMemory;

    public CollectionAccessor(Func<TRecord, TItem[]> getItems) => _getItemsArray = getItems;

    public CollectionAccessor(Func<TRecord, List<TItem>> getItems) => _getItemsList = getItems;

    public CollectionAccessor(Func<TRecord, IEnumerable<TItem>> getItems) => _getItemsCollection = getItems;

    public CollectionAccessor(Func<TRecord, ReadOnlyMemory<TItem>> getItems) => _getItemsMemory = getItems;

    public bool CanUseSpan => _getItemsCollection == null;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<TItem> GetSpan(TRecord record)
    {
        if (_getItemsArray != null)
            return _getItemsArray(record);

        if (_getItemsList != null)
            return CollectionsMarshal.AsSpan(_getItemsList(record));

        return _getItemsMemory!(record).Span;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IEnumerable<TItem> GetCollection(TRecord record)
        => _getItemsCollection!(record);
}