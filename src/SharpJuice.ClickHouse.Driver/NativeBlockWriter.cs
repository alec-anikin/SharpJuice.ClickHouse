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

    public static void Write(Stream stream, IReadOnlyList<IClickHouseColumnWriter> columns, int rowCount)
    {
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        // HTTP inserts are parsed with server_revision = 0, so no BlockInfo and no
        // custom serialization flag precede the counts, unlike the native TCP protocol:
        // https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Formats/Impl/NativeFormat.cpp
        // Write7BitEncodedInt is the same wire encoding as ClickHouse varuint.
        writer.Write7BitEncodedInt(columns.Count);
        writer.Write7BitEncodedInt(rowCount);

        var buffer = ArrayPool<byte>.Shared.Rent(InitialBufferSize);
        try
        {
            foreach (var column in columns)
            {
                // BinaryWriter.Write(string) = varuint byte length + UTF-8, exactly the CH String encoding
                writer.Write(column.ColumnName);
                writer.Write(column.ColumnType);

                // NativeReader reads column data only when rows > 0
                if (rowCount == 0)
                    continue;

                WritePrefix(writer, column, ref buffer);
                WriteData(writer, column, rowCount, ref buffer);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    // WritePrefix/WriteNext contract (SequenceSize.Empty = buffer too small, retry with a bigger one):
    // https://github.com/Octonica/ClickHouseClient/blob/master/src/Octonica.ClickHouseClient/Protocol/IClickHouseColumnWriter.cs

    private static void WritePrefix(BinaryWriter writer, IClickHouseColumnWriter column, ref byte[] buffer)
    {
        while (true)
        {
            var size = column.WritePrefix(buffer);

            // The prefix counts as a single element, so Elements is either 0 or 1
            if (size.Elements == 1)
            {
                writer.Write(buffer, 0, size.Bytes);
                return;
            }

            if (size.Elements != 0)
                throw new InvalidOperationException(
                    $"Column writer returned an unexpected number of prefixes: {size.Elements}.");

            Grow(ref buffer);
        }
    }

    private static void WriteData(BinaryWriter writer, IClickHouseColumnWriter column, int rowCount, ref byte[] buffer)
    {
        while (rowCount > 0)
        {
            var size = column.WriteNext(buffer);

            // Grow and retry, same as Octonica's own ClickHouseBinaryProtocolWriter.WriteRaw.
            // Bytes > 0 with Elements == 0 is a valid result (headers/metadata): write it and continue.
            if (size is { Bytes: 0, Elements: 0 })
            {
                Grow(ref buffer);
                continue;
            }

            writer.Write(buffer, 0, size.Bytes);
            rowCount -= size.Elements;
        }
    }

    private static void Grow(ref byte[] buffer)
    {
        var newBuffer = ArrayPool<byte>.Shared.Rent(buffer.Length * 2);
        ArrayPool<byte>.Shared.Return(buffer);
        buffer = newBuffer;
    }
}
