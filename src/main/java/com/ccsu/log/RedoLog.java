package com.ccsu.log;

import com.ccsu.common.Error;
import com.ccsu.utils.Bytes;
import com.ccsu.utils.Parser;

import java.util.Arrays;

public class RedoLog {
    public static final byte TYPE_INSERT=0;
    public static final byte TYPE_UPDATE=1;

    public static final int LOG_TYPE_OFFSET=0;
    public static final int XID_OFFSET=LOG_TYPE_OFFSET+1;
    public static final int PAGE_NUMBER_OFFSET=XID_OFFSET+8;
    public static final int PAGE_OFFSET_OFFSET=PAGE_NUMBER_OFFSET+4;
    public static final int DATA_OFFSET=PAGE_OFFSET_OFFSET+4;


    private byte type;
    private long xid;
    private int pgno;
    private int offset;
    private byte[] oldRaw;
    private byte[] raw;

    public byte getType() {
        return type;
    }

    public long getXid() {
        return xid;
    }

    public int getPgno() {
        return pgno;
    }

    public int getOffset() {
        return offset;
    }

    public byte[] getOldRaw() {
        return oldRaw;
    }

    public byte[] getRaw() {
        return raw;
    }

    public static RedoLog parseRedoLog(byte[] log) throws Exception {
        byte type=log[0];
        long xid = Parser.parseLong(Arrays.copyOfRange(log,XID_OFFSET,XID_OFFSET+8));
        int pgno = Parser.parseInt(Arrays.copyOfRange(log,PAGE_NUMBER_OFFSET,PAGE_NUMBER_OFFSET+4));
        int offset = Parser.parseInt(Arrays.copyOfRange(log,PAGE_OFFSET_OFFSET,PAGE_OFFSET_OFFSET+4));
        byte[] raw=null;
        byte[] oldRaw=null;
        if(type==TYPE_INSERT){
            raw=Arrays.copyOfRange(log,DATA_OFFSET,log.length);
        }else if(type==TYPE_UPDATE){
            oldRaw=Arrays.copyOfRange(log,DATA_OFFSET,(log.length-DATA_OFFSET)/2+DATA_OFFSET);
            raw=Arrays.copyOfRange(log,(log.length-DATA_OFFSET)/2+DATA_OFFSET,log.length);
        }else{
            throw Error.BadLogFileException;
        }
        return new RedoLog(type,xid,pgno,offset,oldRaw,raw);

    }






    public RedoLog(byte type, long xid, int pgno, int offset, byte[] oldRaw,byte[] raw) {
        this.type=type;
        this.xid = xid;
        this.pgno=pgno;
        this.offset=offset;
        this.oldRaw=oldRaw;
        this.raw=raw;
    }

}
