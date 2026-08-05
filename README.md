# Writing .Net objects to ClickHouse #

[![NuGet](https://img.shields.io/nuget/v/SharpJuice.ClickHouse.svg)](https://www.nuget.org/packages/SharpJuice.ClickHouse/)


[Octonica.ClickHouseClient](https://github.com/Octonica/ClickHouseClient) extension for easily writing objects to ClickHouse using bulk insert and ArrayPool for high performance and low memory allocation. 

Works over either ClickHouse driver — see Registration.


## Registration

There are two packages, one per driver:

| Package | Driver | Transport |
|---|---|---|
| `SharpJuice.ClickHouse` | [Octonica.ClickHouseClient](https://github.com/Octonica/ClickHouseClient) | native TCP, port 9000 |
| `SharpJuice.ClickHouse.Driver` | [ClickHouse.Driver](https://github.com/ClickHouse/clickhouse-cs) | HTTP, port 8123 |

Only the registration differs — `For<T>()`, `AddColumn`, `AddNestedColumn`, `ArrayJoin` and `ITableWriter<T>` are the same types from `SharpJuice.ClickHouse`.

```csharp
    using SharpJuice.Clickhouse;

    ClickHouseConnectionSettings connectionSettings = ...;

    services.AddSingleton<IClickHouseConnectionFactory>(new ClickHouseConnectionFactory(connectionSettings));
    services.AddSingleton<ITableWriterBuilder, TableWriterBuilder>();	
    services.AddSingleton<ClickHouseRepository>();	
```

```csharp
    using SharpJuice.Clickhouse.Driver;

    var settings = new ClickHouseClientSettings("Host=localhost;Port=8123;Username=default");

    // the factory owns a ClickHouseClient, so let the container dispose it
    services.AddSingleton<IClickHouseConnectionFactory>(sp => new ClickHouseConnectionFactory(settings));
    services.AddSingleton<ITableWriterBuilder, TableWriterBuilder>();
    services.AddSingleton<ClickHouseRepository>();
```

For ClickHouse servers below 25.x set `JsonReadMode = JsonReadMode.None` and `JsonWriteMode = JsonWriteMode.None` in `ClickHouseClientSettings`.

Both interfaces are named `IClickHouseConnectionFactory` and live in different namespaces, so an application can use both drivers at once — for example to migrate table by table:

```csharp
    services.AddKeyedSingleton<ITableWriterBuilder>("tcp",  (_, _) => new Clickhouse.TableWriterBuilder(octonicaFactory));
    services.AddKeyedSingleton<ITableWriterBuilder>("http", (_, _) => new Clickhouse.Driver.TableWriterBuilder(driverFactory));
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


## Performance and memory allocation benchmark (.net 10)

[Benchmark source code](https://github.com/alec-anikin/SharpJuice.Clickhouse/blob/main/benchmark/Benchmarks/Insert.cs)

All methods insert the same objects (4 scalar columns + 3 Nested groups) into the same table.
Apple M3 Pro, ClickHouse 24.3 in local Docker.

Summary — Nested data, 30000 rows:

| Path                                        | Mean         | Allocated  | Gen1 / Gen2 |
|-------------------------------------------- |-------------:|-----------:|------------:|
| **SharpJuice + ClickHouse.Driver**          | **103.9 ms** | **6.1 MB** |   **0 / 0** |
| raw ClickHouse.Driver (`InsertBinaryAsync`) |     162.9 ms |   113.6 MB | 4500 / 1500 |
| SharpJuice + Octonica                       |     399.5 ms |    13.9 MB |       0 / 0 |
| raw Octonica (`ColumnWriter`)               |     420.7 ms |    52.8 MB | 3000 / 1000 |

Full results below.
Columns: `Nested` — via `AddNestedColumn`, `flat arrays` — array columns built by hand (`.ToArray()` in lambdas).
`InsertBinaryAsync` takes `object[]` per row, `ColumnWriter` takes a dictionary of column arrays — both without SharpJuice.

| API                 | Driver                  | Columns     | ObjectsCount | Mean       | Error     | StdDev     | Gen0       | Gen1      | Gen2      | Allocated    |
|-------------------- |------------------------ |------------ |------------- |-----------:|----------:|-----------:|-----------:|----------:|----------:|-------------:|
| `InsertBinaryAsync` | ClickHouse.Driver, HTTP | —           | 100          |   2.937 ms | 0.1690 ms |  0.4902 ms |   105.4688 |   66.4063 |   58.5938 |    668.68 KB |
| SharpJuice          | ClickHouse.Driver, HTTP | Nested      | 100          |   3.082 ms | 0.1264 ms |  0.3524 ms |          - |         - |         - |     63.22 KB |
| SharpJuice          | ClickHouse.Driver, HTTP | flat arrays | 100          |   3.352 ms | 0.1776 ms |  0.4981 ms |    15.6250 |         - |         - |    168.96 KB |
| `ColumnWriter`      | Octonica, TCP           | —           | 100          |   4.256 ms | 0.2153 ms |  0.6109 ms |    31.2500 |         - |         - |    320.08 KB |
| SharpJuice          | Octonica, TCP           | Nested      | 100          |   4.670 ms | 0.3877 ms |  1.0677 ms |    15.6250 |         - |         - |    204.41 KB |
| SharpJuice          | Octonica, TCP           | flat arrays | 100          |   5.678 ms | 0.5668 ms |  1.5987 ms |    31.2500 |         - |         - |    310.11 KB |
| SharpJuice          | ClickHouse.Driver, HTTP | flat arrays | 1000         |   6.094 ms | 0.2734 ms |  0.7846 ms |   171.8750 |   78.1250 |         - |   1453.76 KB |
| SharpJuice          | ClickHouse.Driver, HTTP | Nested      | 1000         |   7.074 ms | 0.9249 ms |  2.5164 ms |    23.4375 |         - |         - |    235.95 KB |
| `InsertBinaryAsync` | ClickHouse.Driver, HTTP | —           | 1000         |   9.336 ms | 0.5346 ms |  1.4902 ms |   515.6250 |  265.6250 |   46.8750 |   4149.54 KB |
| SharpJuice          | Octonica, TCP           | flat arrays | 1000         |  16.743 ms | 0.3335 ms |  0.6014 ms |   281.2500 |  125.0000 |         - |   2362.93 KB |
| `ColumnWriter`      | Octonica, TCP           | —           | 1000         |  16.905 ms | 0.4849 ms |  1.3677 ms |   281.2500 |  125.0000 |         - |   2472.62 KB |
| SharpJuice          | Octonica, TCP           | Nested      | 1000         |  17.154 ms | 0.4038 ms |  1.1323 ms |   125.0000 |   46.8750 |         - |   1142.78 KB |
| SharpJuice          | ClickHouse.Driver, HTTP | Nested      | 10000        |  36.902 ms | 1.0664 ms |  2.9372 ms |   230.7692 |   76.9231 |         - |   2099.59 KB |
| SharpJuice          | ClickHouse.Driver, HTTP | flat arrays | 10000        |  44.311 ms | 0.4230 ms |  0.3750 ms |  1875.0000 |  750.0000 |  125.0000 |  14295.41 KB |
| `InsertBinaryAsync` | ClickHouse.Driver, HTTP | —           | 10000        |  54.219 ms | 1.0690 ms |  2.0338 ms |  5250.0000 | 2250.0000 |  750.0000 |  38957.01 KB |
| SharpJuice          | ClickHouse.Driver, HTTP | Nested      | 30000        | 103.893 ms | 2.0740 ms |  4.0938 ms |   500.0000 |         - |         - |   6245.27 KB |
| SharpJuice          | Octonica, TCP           | Nested      | 10000        | 133.688 ms | 2.2981 ms |  1.9190 ms |  1000.0000 |  500.0000 |         - |   9086.04 KB |
| `ColumnWriter`      | Octonica, TCP           | —           | 10000        | 141.161 ms | 2.2524 ms |  3.5726 ms |  3000.0000 | 1500.0000 |  500.0000 |  22380.29 KB |
| SharpJuice          | Octonica, TCP           | flat arrays | 10000        | 143.655 ms | 2.8176 ms |  4.9348 ms |  2500.0000 | 1000.0000 |         - |   21280.2 KB |
| SharpJuice          | ClickHouse.Driver, HTTP | flat arrays | 30000        | 147.155 ms | 2.9414 ms |  5.0738 ms |  5250.0000 | 2000.0000 |  250.0000 |  42810.55 KB |
| `InsertBinaryAsync` | ClickHouse.Driver, HTTP | —           | 30000        | 162.917 ms | 3.2365 ms |  7.0359 ms | 15500.0000 | 4500.0000 | 1500.0000 | 116302.75 KB |
| SharpJuice          | Octonica, TCP           | Nested      | 30000        | 399.454 ms | 6.7228 ms |  9.4245 ms |  1000.0000 |         - |         - |  14228.48 KB |
| `ColumnWriter`      | Octonica, TCP           | —           | 30000        | 420.657 ms | 8.1707 ms | 11.4542 ms |  7000.0000 | 3000.0000 | 1000.0000 |   54092.7 KB |
| SharpJuice          | Octonica, TCP           | flat arrays | 30000        | 428.660 ms | 8.5627 ms | 13.5813 ms |  6000.0000 | 2000.0000 |         - |  50799.29 KB |



Thanks to [@deniskuzmin](https://github.com/deniskuzmin) and [@LegaNoga](https://github.com/LegaNoga)
