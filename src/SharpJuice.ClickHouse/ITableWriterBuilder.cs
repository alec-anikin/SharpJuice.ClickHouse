namespace SharpJuice.Clickhouse;

public interface ITableWriterBuilder
{
    ITableWriterBuilder<TSource> For<TSource>(string tableName);
}

public interface ITableWriterBuilder<TSource> : ITableWriterBuilderBase<TSource>
{
    ITableWriterBuilder<TSource> AddColumn<TColumn>(string name, Func<TSource, TColumn> getValue);

    IJoinedTableWriterBuilder<TSource> ArrayJoin<TItem>(
        Func<TSource, IReadOnlyCollection<TItem>> getItems,
        Action<INestedTableBuilder<TItem>> configuration);
}

public interface INestedTableWriterBuilder<TSource> : ITableWriterBuilderBase<TSource>
{
    INestedTableWriterBuilder<TSource> AddColumn<TColumn>(string name, Func<TSource, TColumn> getValue);
}

public interface ITableWriterBuilderBase<TSource>
{
    ITableWriter<TSource> Build();

    internal INestedTableWriterBuilder<TSource> AddNestedColumn<TItem>(
        string? name,
        CollectionAccessor<TSource, TItem> collectionAccessor,
        Action<INestedTableBuilder<TItem>> configuration);
}

public interface IJoinedTableWriterBuilder<TSource>
{
    IJoinedTableWriterBuilder<TSource> AddColumn<TColumn>(string name, Func<TSource, TColumn> getValue);

    ITableWriter<TSource> Build();
}

public interface INestedTableBuilder<out TItem>
{
    INestedTableBuilder<TItem> AddColumn<TColumn>(string name, Func<TItem, TColumn> getValue);
}

public static class BuilderExtensions
{
    public static INestedTableWriterBuilder<TSource> AddNestedColumn<TSource, TItem>(
        this ITableWriterBuilderBase<TSource> builder,
        Func<TSource, IEnumerable<TItem>> getItems,
        Action<INestedTableBuilder<TItem>> configuration)
        => builder.AddNestedColumn(default, getItems, configuration);

    public static INestedTableWriterBuilder<TSource> AddNestedColumn<TSource, TItem>(
        this ITableWriterBuilderBase<TSource> builder,
        Func<TSource, TItem[]> getItems,
        Action<INestedTableBuilder<TItem>> configuration)
        => builder.AddNestedColumn(default, getItems, configuration);

    public static INestedTableWriterBuilder<TSource> AddNestedColumn<TSource, TItem>(
        this ITableWriterBuilderBase<TSource> builder,
        Func<TSource, List<TItem>> getItems,
        Action<INestedTableBuilder<TItem>> configuration)
        => builder.AddNestedColumn(default, getItems, configuration);

    public static INestedTableWriterBuilder<TSource> AddNestedColumn<TSource, TItem>(
        this ITableWriterBuilderBase<TSource> builder,
        Func<TSource, ReadOnlyMemory<TItem>> getItems,
        Action<INestedTableBuilder<TItem>> configuration)
        => builder.AddNestedColumn(default, getItems, configuration);

    public static INestedTableWriterBuilder<TSource> AddNestedColumn<TSource, TItem>(
        this ITableWriterBuilderBase<TSource> builder,
        string? name,
        Func<TSource, TItem[]> getItems,
        Action<INestedTableBuilder<TItem>> configuration)
        => builder.AddNestedColumn(name, new(getItems), configuration);

    public static INestedTableWriterBuilder<TSource> AddNestedColumn<TSource, TItem>(
        this ITableWriterBuilderBase<TSource> builder,
        string? name,
        Func<TSource, List<TItem>> getItems,
        Action<INestedTableBuilder<TItem>> configuration)
        => builder.AddNestedColumn(name, new(getItems), configuration);

    public static INestedTableWriterBuilder<TSource> AddNestedColumn<TSource, TItem>(
        this ITableWriterBuilderBase<TSource> builder,
        string? name,
        Func<TSource, ReadOnlyMemory<TItem>> getItems,
        Action<INestedTableBuilder<TItem>> configuration)
        => builder.AddNestedColumn(name, new(getItems), configuration);

    public static INestedTableWriterBuilder<TSource> AddNestedColumn<TSource, TItem>(
        this ITableWriterBuilderBase<TSource> builder,
        string? name,
        Func<TSource, IEnumerable<TItem>> getItems,
        Action<INestedTableBuilder<TItem>> configuration)
        => builder.AddNestedColumn(name, new(getItems), configuration);
}