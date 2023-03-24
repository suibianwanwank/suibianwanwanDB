package com.ccsu.dm;

public class Types {
    public static long addressToUid(int pgno, int offset) {
        long u0 = (long)pgno;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}
