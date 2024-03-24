# Writing .Net objects to ClickHouse #

[![NuGet](https://img.shields.io/nuget/v/SharpJuice.ClickHouse.svg)](https://www.nuget.org/packages/SharpJuice.ClickHouse/)


[Octonica.ClickHouseClient](https://github.com/Octonica/ClickHouseClient) extension for easily writing objects to ClickHouse using bulk insert and ArrayPool for high performance and low memory allocation. 


## Registration

```csharp
    ClickHouseConnectionSettings connectionSettings = ...;

    services.AddSingleton<IClickHouseConnectionFactory>(new ClickHouseConnectionFactory(connectionSettings));
    services.AddSingleton<ITableWriterBuilder, TableWriterBuilder>();	
    services.AddSingleton<ClickHouseRepository>();	
```

## Flat object

```csharp
    public sealed class ClickHouseRepository 
    {
        private readonly ITableWriter<Order> _tableWriter;
        
        public ClickHouseRepository(ITableWriterBuilder tableWriterBuilder)
        {
            _tableWriter = tableWriterBuilder
                .For<Order>("table_name")
                .AddColumn("order_id", a => a.OrderId)
                .AddColumn("user_id", a => a.UserId)
                .AddColumn("created_at", a => a.CreatedAt)                
		.Build();
        }

        public async Task Add(IReadOnlyCollection<Order> orders, CancellationToken token)
        {
	    ...
            await _tableWriter.Insert(orders, token);
	    ...
        }
    }
```

## Nested objects

```csharp
    public sealed class ClickHouseRepository 
    {
        private readonly ITableWriter<Order> _tableWriter;
        
        public ClickHouseRepository(ITableWriterBuilder tableWriterBuilder)
        {
            _tableWriter = tableWriterBuilder
                .For<Order>("table_name")
                .AddColumn("order_id", a => a.OrderId)
                .AddColumn("date", a => a.Date)
                .AddNestedColumn("item", x => x.Items, c => c
                    .AddColumn("id", x => x.Id)
                    .AddColumn("quantity", x => x.Quantity)
                    .AddColumn("name", x => x.Name)
                    .AddColumn("price", x => x.Price))
                .AddNestedColumn(x => x.Discounts, c => c
                    .AddColumn("discount.id", x => x.Id)
                    .AddColumn("discount.name", x => x.Name)
                    .AddColumn("discount.value", x => x.Value))                
                .Build();          
        }

        public async Task Add(IReadOnlyCollection<Order> orders, CancellationToken token)
        {
	    ...
            await _tableWriter.Insert(orders, token);
	    ...
        }
    }
```

## Array join (only one ArrayJoin per writer)

```csharp
    public sealed class ClickHouseRepository 
    {
        private readonly ITableWriter<Order> _tableWriter;
        
        public ClickHouseRepository(ITableWriterBuilder tableWriterBuilder)
        {
            _tableWriter = tableWriterBuilder
                .For<Order>("table_name")
                .AddColumn("order_id", a => a.OrderId)
                .AddColumn("date", a => a.Date)
                .ArrayJoin(a => a.Items, c => c
                    .AddColumn("item_id", x => x.Id)
                    .AddColumn("item_quantity", x => x.Quantity)
                    .AddColumn("item_name", x => x.Name)
                    .AddColumn("item_price", x => x.Price))
		    .Build();
        }

        public async Task Add(IReadOnlyCollection<Order> orders, CancellationToken token)
        {
	    ...
            await _tableWriter.Insert(orders, token);
	    ...
        }
    }
```


## Performance and memory allocation benchmark (.net 8)

[Benchmark source code](https://github.com/alec-anikin/SharpJuice.Clickhouse/blob/main/benchmark/Benchmarks/Insert.cs)


| Method                        | ObjectsCount | Mean      | Error     | StdDev    | Gen0       | Gen1      | Gen2      | Allocated   |
|------------------------------ |------------- |----------:|----------:|----------:|-----------:|----------:|----------:|------------:|
| NestedObject_Writer           | 1000         |  71.87 ms |  1.425 ms |  2.176 ms |          - |         - |         - |  1148.29 KB |
| FlatObject_Writer             | 1000         |  73.33 ms |  1.159 ms |  1.028 ms |   375.0000 |  250.0000 |         - |  2837.81 KB |
| ClickhouseClient_ColumnWriter | 1000         |  78.63 ms |  1.631 ms |  4.731 ms |   428.5714 |  142.8571 |         - |  2948.86 KB |
| NestedObject_Writer           | 100          | 101.91 ms |  1.629 ms |  1.523 ms |          - |         - |         - |      205 KB |
| ClickhouseClient_ColumnWriter | 100          | 102.99 ms |  2.008 ms |  2.749 ms |          - |         - |         - |   368.33 KB |
| FlatObject_Writer             | 100          | 103.24 ms |  2.025 ms |  2.633 ms |          - |         - |         - |   358.71 KB |
| NestedObject_Writer           | 10000        | 281.58 ms | 10.143 ms | 29.102 ms |  1000.0000 |         - |         - |  9140.61 KB |
| ClickhouseClient_ColumnWriter | 10000        | 284.90 ms |  9.745 ms | 27.325 ms |  4000.0000 | 1000.0000 |         - | 27643.59 KB |
| FlatObject_Writer             | 10000        | 302.62 ms | 13.242 ms | 38.836 ms |  4000.0000 | 1000.0000 |         - | 26020.02 KB |
| NestedObject_Writer           | 30000        | 712.17 ms | 23.182 ms | 67.622 ms |  2000.0000 | 1000.0000 |         - |  14370.6 KB |
| ClickhouseClient_ColumnWriter | 30000        | 775.75 ms | 22.886 ms | 66.396 ms | 11000.0000 | 5000.0000 | 2000.0000 | 69326.91 KB |
| FlatObject_Writer             | 30000        | 781.20 ms | 24.974 ms | 72.057 ms | 10000.0000 | 3000.0000 |         - | 65002.87 KB |



Thanks to [@deniskuzmin](https://github.com/deniskuzmin) and [@LegaNoga](https://github.com/LegaNoga)
