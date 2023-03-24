package com.ccsu.page;

import com.ccsu.utils.Parser;

import java.util.Arrays;

public class PageManager {



    private static final short FREE_OFFSET = 0;

    private static final short DATA_OFFSET = 4;

    public static final int PAGE_SIZE = 1 << 13;

    public static final int MAX_FREE_SPACE = PAGE_SIZE-DATA_OFFSET;

//    public static final int MAX_FREE_SPACE ;

    public static byte[] initRaw() {
        byte[] raw = new byte[PAGE_SIZE];
        setFSO(raw, DATA_OFFSET);
        return raw;
    }

    public static int insert(Page pg, byte[] raw) {

        int offset = getFSO(pg);
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        setFSO(pg.getData(), offset + raw.length);
        return offset;
    }

    public static int insert(Page pg, byte[] raw,int offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        return offset;
    }

    private static void setFSO(byte[] raw, int ofData) {
        System.arraycopy(Parser.unParseInt(ofData), 0, raw, FREE_OFFSET, DATA_OFFSET);
    }


    public static int getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    private static int getFSO(byte[] raw) {
        return Parser.parseInt(Arrays.copyOfRange(raw, 0, 4));
    }

    public static int getFreeSpace(Page pg) {
        return PAGE_SIZE - getFSO(pg.getData());
    }

    public static void recoverInsert(Page pg,  int offset,byte[] raw) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        int rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), offset+raw.length);
        }
    }

    // 将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page pg,int offset, byte[] raw) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }


}
