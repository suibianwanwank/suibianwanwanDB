package com.ccsu.utils;

public class Bytes {
    /**
     * Custom utility classes to concatenate any number of byte arrays
     * @param values
     * @return
     */
    public static byte[] concat(byte[]... values) {
        int lengthByte = 0;
        for (byte[] value : values) {
            lengthByte += value.length;
        }
        byte[] allBytes = new byte[lengthByte];
        int countLength = 0;
        for (byte[] b : values) {
            System.arraycopy(b, 0, allBytes, countLength, b.length);
            countLength += b.length;
        }
        return allBytes;
    }

}
