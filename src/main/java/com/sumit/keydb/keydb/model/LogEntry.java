package com.sumit.keydb.keydb.model;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class LogEntry {
    public static final int HEADER_SIZE = 4 + 8 + 4 + 4;

    public static byte[] serialize(byte[] key, byte[] value) {
        CRC32 crc = new CRC32();
        crc.update(key);
        if (value != null) {
            crc.update(value);
        }

        ByteBuffer buffer = ByteBuffer.allocate(
                HEADER_SIZE + key.length + (value == null ? 0 : value.length)
        );
        buffer.putInt((int) crc.getValue()); // 4
        buffer.putLong(System.currentTimeMillis()); // 8
        buffer.putInt(key.length); // 4
        buffer.putInt(value == null ? -1 : value.length); // 4
        buffer.put(key);
        if (value != null) {
            buffer.put(value);
        }
        return buffer.array();
    }
}