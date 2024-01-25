```

BenchmarkDotNet v0.13.12, Windows 10 (10.0.19045.3930/22H2/2022Update)
11th Gen Intel Core i7-1185G7 3.00GHz, 1 CPU, 8 logical and 4 physical cores
.NET SDK 8.0.101
  [Host]     : .NET 8.0.1 (8.0.123.58001), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI
  DefaultJob : .NET 8.0.1 (8.0.123.58001), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI


```
| Method         | records           | Mean        | Error     | StdDev     | Median      | Gen0       | Gen1      | Allocated   |
|--------------- |------------------ |------------:|----------:|-----------:|------------:|-----------:|----------:|------------:|
| LowAllocating  | TestObject[100]   |    62.42 ms |  1.239 ms |   2.640 ms |    62.17 ms |          - |         - |   267.02 KB |
| HighAllocating | TestObject[100]   |    62.87 ms |  1.239 ms |   2.797 ms |    61.93 ms |          - |         - |   419.05 KB |
| LowAllocating  | TestObject[1000]  |   137.65 ms |  2.749 ms |   4.198 ms |   138.79 ms |   250.0000 |         - |  1808.32 KB |
| HighAllocating | TestObject[1000]  |   138.68 ms |  2.737 ms |   4.935 ms |   140.43 ms |   500.0000 |  250.0000 |  3502.59 KB |
| LowAllocating  | TestObject[10000] |   497.81 ms | 14.665 ms |  42.313 ms |   497.42 ms |  2000.0000 | 1000.0000 | 14584.94 KB |
| HighAllocating | TestObject[10000] |   527.56 ms | 16.642 ms |  48.808 ms |   514.63 ms |  5000.0000 | 2000.0000 | 31463.55 KB |
| LowAllocating  | TestObject[30000] | 1,358.49 ms | 44.110 ms | 129.366 ms | 1,316.87 ms |  3000.0000 | 1000.0000 | 21097.38 KB |
| HighAllocating | TestObject[30000] | 1,386.49 ms | 41.162 ms | 118.101 ms | 1,352.50 ms | 11000.0000 | 3000.0000 | 71728.45 KB |
