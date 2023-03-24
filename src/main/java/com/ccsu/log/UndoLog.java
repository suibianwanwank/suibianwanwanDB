package com.ccsu.log;

import com.ccsu.utils.Bytes;
import com.ccsu.utils.Parser;

import java.util.Arrays;

public class UndoLog {
    public static final byte TYPE_INSERT=0;
    public static final byte TYPE_UPDATE=1;
    public static final byte TYPE_DELETE=2;

    public static final int LOG_TYPE_OFFSET=0;
    public static final int XID_OFFSET=LOG_TYPE_OFFSET+1;
    public static final int XID_UID_OFFSET=XID_OFFSET+8;
    public static final int DATA_UID_OFFSET=XID_UID_OFFSET+8;
    public static final int DATA_SIZE_OFFSET=DATA_UID_OFFSET+8;
    public static final int DATA_OFFSET=DATA_SIZE_OFFSET+4;

    private byte type;
    private long xid;

    private long xidUid;
    private long dataUid;

    private byte[] data;

    public byte getType() {
        return type;
    }

    public long getXid() {
        return xid;
    }

    public long getXidUid() {
        return xidUid;
    }

    public long getDataUid() {
        return dataUid;
    }

    public byte[] getData() {
        return data;
    }

    public UndoLog(byte type, long xid, long xidUid, long dataUid, byte[] data) {
        this.type=type;
        this.xid = xid;
        this.xidUid = xidUid;
        this.dataUid = dataUid;
        this.data = data;
    }

    public static UndoLog parseUndoLog(byte[] raw){
        byte type=Arrays.copyOfRange(raw,LOG_TYPE_OFFSET,LOG_TYPE_OFFSET+1)[0];
        long xid = Parser.parseLong(Arrays.copyOfRange(raw, XID_OFFSET, XID_OFFSET+8));
        long xidUid = Parser.parseLong(Arrays.copyOfRange(raw, XID_UID_OFFSET, XID_UID_OFFSET+8));
        long dataUid = Parser.parseLong(Arrays.copyOfRange(raw, DATA_UID_OFFSET, DATA_UID_OFFSET+8));
        int dataSize= Parser.parseInt(Arrays.copyOfRange(raw, DATA_SIZE_OFFSET, DATA_SIZE_OFFSET+4));
        byte[] data= Arrays.copyOfRange(raw, DATA_OFFSET, DATA_OFFSET+dataSize);

        return new UndoLog(type,xid,xidUid,dataUid,data);
    }

    public static byte[] unParseUndoLog(UndoLog log){
        byte[] byteType = new byte[]{log.type};
        byte [] byteXid=Parser.unParseLong(log.xid);
        byte [] byteXidUid=Parser.unParseLong(log.xidUid);
        byte [] byteDataUid=Parser.unParseLong(log.dataUid);
        byte [] byteDataSize=Parser.unParseInt(log.data.length);

        return Bytes.concat(byteType,byteXid,byteXidUid,byteDataUid,byteDataSize,log.data);
    }








}
