using System.Buffers;
using System.Text;
using Octonica.ClickHouseClient.Protocol;

namespace SharpJuice.Clickhouse.Driver;

/// <summary>
/// Writes a block in the ClickHouse Native format as expected over HTTP.
/// Format overview: https://clickhouse.com/docs/interfaces/formats/Native
/// </summary>
internal static class NativeBlockWriter
{
    private const int InitialBufferSize = 64 * 1024;

    public static async Task WriteAsync(
        Stream stream,
        IReadOnlyList<IClickHouseColumnWriter> columns,
        int rowCount,
        CancellationToken cancellationToken)
    {
        await using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        // HTTP inserts are parsed with server_revision = 0, so no BlockInfo and no
        // custom serialization flag precede the counts, unlike the native TCP protocol:
        // https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Formats/Impl/NativeFormat.cpp
        // Write7BitEncodedInt is the same wire encoding as ClickHouse varuint.
        writer.Write7BitEncodedInt(columns.Count);
        writer.Write7BitEncodedInt(rowCount);

        using var buffer = new GrowingBuffer(InitialBufferSize);

        foreach (var column in columns)
        {
            // BinaryWriter.Write(string) = varuint byte length + UTF-8, exactly the CH String encoding
            writer.Write(column.ColumnName);
            writer.Write(column.ColumnType);

            // NativeReader reads column data only when rows > 0
            if (rowCount == 0)
                continue;

            await WritePrefixAsync(stream, column, buffer, cancellationToken);
            await WriteDataAsync(stream, column, rowCount, buffer, cancellationToken);
        }
    }

    // WritePrefix/WriteNext contract (SequenceSize.Empty = buffer too small, retry with a bigger one):
    // https://github.com/Octonica/ClickHouseClient/blob/master/src/Octonica.ClickHouseClient/Protocol/IClickHouseColumnWriter.cs
    private static async Task WritePrefixAsync(
        Stream stream,
        IClickHouseColumnWriter column,
        GrowingBuffer buffer,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            var size = column.WritePrefix(buffer.Buffer);

            // The prefix counts as a single element, so Elements is either 0 or 1
            if (size.Elements == 1)
            {
                await stream.WriteAsync(buffer.AsMemory(size.Bytes), cancellationToken);
                return;
            }

            if (size.Elements != 0)
                throw new InvalidOperationException(
                    $"Column writer returned an unexpected number of prefixes: {size.Elements}.");

            buffer.Grow();
        }
    }

    private static async Task WriteDataAsync(
        Stream stream,
        IClickHouseColumnWriter column,
        int rowCount,
        GrowingBuffer buffer,
        CancellationToken cancellationToken)
    {
        while (rowCount > 0)
        {
            var size = column.WriteNext(buffer.Buffer);

            // Grow and retry, same as Octonica's own ClickHouseBinaryProtocolWriter.WriteRaw.
            // Bytes > 0 with Elements == 0 is a valid result (headers/metadata): write it and continue.
            if (size is { Bytes: 0, Elements: 0 })
            {
                buffer.Grow();
                continue;
            }

            await stream.WriteAsync(buffer.AsMemory(size.Bytes), cancellationToken);
            rowCount -= size.Elements;
        }
    }
    
    private sealed class GrowingBuffer(int initialSize) : IDisposable
    {
        private byte[] _buffer = ArrayPool<byte>.Shared.Rent(initialSize);

        public Span<byte> Buffer => _buffer;

        public ReadOnlyMemory<byte> AsMemory(int length) => _buffer.AsMemory(0, length);

        public void Grow()
        {
            var grown = ArrayPool<byte>.Shared.Rent(_buffer.Length * 2);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = grown;
        }

        public void Dispose() => ArrayPool<byte>.Shared.Return(_buffer);
    }
}
