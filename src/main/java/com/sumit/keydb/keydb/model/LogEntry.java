package com.sumit.keydb.keydb.model;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class LogEntry {
    public static final int HEADER_SIZE = 4 + 8 + 4 + 4;

    public static byte[] serializeWithTimestamp(byte[] key, byte[] value, long timestamp) {
        int valueSize = value == null ? -1 : value.length;

        ByteBuffer buffer = ByteBuffer.allocate(
                HEADER_SIZE + key.length + Math.max(valueSize, 0)
        );

        buffer.putInt(0);
        buffer.putLong(timestamp);
        buffer.putInt(key.length);
        buffer.putInt(valueSize);
        buffer.put(key);
        if (value != null) {
            buffer.put(value);
        }

        // Compute CRC over everything except first 4 bytes
        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 4, buffer.position() - 4);
        buffer.putInt(0, (int) crc.getValue());
        return buffer.array();
    }
}