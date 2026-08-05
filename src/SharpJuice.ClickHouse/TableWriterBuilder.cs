using SharpJuice.Clickhouse.TableSchema;

namespace SharpJuice.Clickhouse;

public sealed class TableWriterBuilder : ITableWriterBuilder
{
    private readonly ITableSinkFactory _sinkFactory;

    public TableWriterBuilder(IClickHouseConnectionFactory connectionFactory)
        : this(new OctonicaTableSinkFactory(connectionFactory))
    {
    }

    internal TableWriterBuilder(ITableSinkFactory sinkFactory)
    {
        _sinkFactory = sinkFactory;
    }

    public ITableWriterBuilder<TSource> For<TSource>(string tableName)
        => new Builder<TSource>(tableName, _sinkFactory);

    private sealed class Builder<TSource> :
        ITableWriterBuilder<TSource>,
        INestedTableWriterBuilder<TSource>
    {
        private readonly string _tableName;
        private readonly ITableSinkFactory _sinkFactory;
        private readonly List<IColumnDefinition<TSource>> _columnDefinitions = new();

        public Builder(string tableName, ITableSinkFactory sinkFactory)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentException("Table name must be specified", nameof(tableName));

            _tableName = tableName;
            _sinkFactory = sinkFactory;
        }

        public ITableWriterBuilder<TSource> AddColumn<TColumn>(string name, Func<TSource, TColumn> getValue)
        {
            _columnDefinitions.Add(new ColumnDefinition<TSource, TColumn>(name, getValue));
            return this;
        }

        INestedTableWriterBuilder<TSource> INestedTableWriterBuilder<TSource>.AddColumn<TColumn>(
            string name,
            Func<TSource, TColumn> getValue)
        {
            _columnDefinitions.Add(new ColumnDefinition<TSource, TColumn>(name, getValue));
            return this;
        }

        public INestedTableWriterBuilder<TSource> AddNestedColumn<TItem>(
            string? name,
            CollectionAccessor<TSource, TItem> collectionAccessor,
            Action<INestedTableBuilder<TItem>> configuration)
        {
            var columns = new ArrayColumnCollection<TItem>();
            configuration(columns);

            _columnDefinitions.Add(new NestedColumnDefinition<TSource, TItem>(
                name ?? string.Empty,
                collectionAccessor,
                columns.ToArray()));

            return this;
        }

        public IJoinedTableWriterBuilder<TSource> ArrayJoin<TItem>(
            Func<TSource, IReadOnlyCollection<TItem>> getItems,
            Action<INestedTableBuilder<TItem>> configuration)
        {
            var columns = new ColumnCollection<TItem>();
            configuration(columns);

            return new JoinedBuilder<TSource, TItem>(
                _tableName,
                _sinkFactory,
                _columnDefinitions,
                columns,
                getItems);
        }

        public ITableWriter<TSource> Build()
        {
            if (_columnDefinitions.Count == 0)
                throw new InvalidOperationException("Column definitions collection is empty");

            var columnNames = _columnDefinitions.SelectMany(x => x.GetNames()).ToArray();

            return new TableWriter<TSource>(
                new TableBuilder<TSource>(_columnDefinitions),
                _sinkFactory.Create(_tableName, columnNames));
        }
    }

    private sealed class JoinedBuilder<TSource, TItem> :
        IJoinedTableWriterBuilder<TSource>
    {
        private readonly string _tableName;
        private readonly ITableSinkFactory _sinkFactory;
        private readonly List<IColumnDefinition<TSource>> _columnDefinitions;
        private readonly Func<TSource, IReadOnlyCollection<TItem>> _getItems;
        private readonly List<IColumnDefinition<TItem>> _itemColumnDefinitions;

        public JoinedBuilder(
            string tableName,
            ITableSinkFactory sinkFactory,
            List<IColumnDefinition<TSource>> columnDefinitions,
            List<IColumnDefinition<TItem>> itemColumnDefinitions,
            Func<TSource, IReadOnlyCollection<TItem>> getItems)
        {
            _tableName = tableName;
            _sinkFactory = sinkFactory;
            _columnDefinitions = columnDefinitions;
            _itemColumnDefinitions = itemColumnDefinitions;
            _getItems = getItems;
        }

        public IJoinedTableWriterBuilder<TSource> AddColumn<TColumn>(string name, Func<TSource, TColumn> getValue)
        {
            _columnDefinitions.Add(new ColumnDefinition<TSource, TColumn>(name, getValue));
            return this;
        }

        public ITableWriter<TSource> Build()
        {
            if (_columnDefinitions.Count == 0)
                throw new InvalidOperationException("Column definitions collection is empty");

            if (_itemColumnDefinitions.Count == 0)
                throw new InvalidOperationException("Nested item column definitions collection is empty");

            var columnNames = _columnDefinitions.SelectMany(x => x.GetNames())
                .Concat(_itemColumnDefinitions.SelectMany(x => x.GetNames()))
                .ToArray();

            return new TableWriter<TSource>(
                new JoinedTableBuilder<TSource, TItem>(_columnDefinitions, _itemColumnDefinitions, _getItems),
                _sinkFactory.Create(_tableName, columnNames));
        }
    }

    private sealed class ColumnCollection<T> :
        List<IColumnDefinition<T>>,
        INestedTableBuilder<T>
    {
        public INestedTableBuilder<T> AddColumn<TColumn>(string name, Func<T, TColumn> getValue)
        {
            Add(new ColumnDefinition<T, TColumn>(name, getValue));
            return this;
        }
    }

    private sealed class ArrayColumnCollection<T> :
        List<IArrayColumnDefinition<T>>,
        INestedTableBuilder<T>
    {
        public INestedTableBuilder<T> AddColumn<TColumn>(string name, Func<T, TColumn> getValue)
        {
            Add(new ArrayColumnDefinition<T, TColumn>(name, getValue));
            return this;
        }
    }
}
