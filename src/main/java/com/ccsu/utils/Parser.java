package com.ccsu.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A utility class for converting various types to and from byte arrays
 */
public class Parser {
    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    public static byte[] unParseInt(int data) {
        return ByteBuffer.allocate(Integer.SIZE/Byte.SIZE).putInt(data).array();
    }

    public static byte[] unParseLong(long data) {
        return ByteBuffer.allocate(Long.SIZE/Byte.SIZE).putLong(data).array();
    }

    public static byte[] unParseString(String str) {
        byte[] l = unParseInt(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4+length));
        return new ParseStringRes(str, length+4);
    }


}
