package com.ccsu.vm;

import com.ccsu.common.Error;
import com.ccsu.dm.DataItem;
import com.ccsu.dm.DataItemImpl;
import com.ccsu.utils.Bytes;
import com.ccsu.utils.Parser;
import com.ccsu.utils.SubArray;

import java.util.Arrays;



/**
 * VM向上层抽象出entry
 * entry结构：
 * [XMIN] [XMAX] [OF_ROLLBACK_POINTER] [data]
 */
public class Entry {

    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN+8;
    private static final int OF_ROLLBACK_POINTER = OF_XMAX+8;
    private static final int OF_DATA = OF_ROLLBACK_POINTER+8;

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;

    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        if(di==null) {
            throw Error.NullEntryException;
        }
        return newEntry(vm, di, uid);
    }

    public static Entry loadEntry(byte[] raw) throws Exception{
        DataItem dataItem1 = DataItemImpl.transBaseDataItem(raw);
        Entry entry=new Entry();
        entry.dataItem=dataItem1;
        return entry;
    }

    public static byte[] wrapEntryRaw(long xid, byte[] data,long pointer) {
        byte[] xmin = Parser.unParseLong(xid);
        byte[] xmax = new byte[8];
        byte[] rollBackPoint=Parser.unParseLong(pointer);
        return Bytes.concat(xmin, xmax, rollBackPoint,data);
    }

    public void release() throws Exception {
        dataItem.release();
    }


    // 以拷贝的形式返回内容
    public byte[] data() {
        dataItem.rLock();
        try {
//            SubArray sa = dataItem.data();
            byte[] data1 = dataItem.data();
            byte[] data = new byte[data1.length-OF_DATA];
            System.arraycopy(data1, OF_DATA, data, 0, data1.length-OF_DATA);
            return data;
        } finally {
            dataItem.runLock();
        }
    }

    public byte[] getEntry(){
        dataItem.rLock();
        try {
//            SubArray sa = dataItem.data();
            byte[] data1 = dataItem.data();
            byte[] data = new byte[data1.length];
            System.arraycopy(data1, 0, data, 0, data.length);
            return data;
        }finally {
            dataItem.runLock();
        }

    }

//    public SubArray data2() {
//        dataItem.rLock();
//        try {
//            SubArray sa = dataItem.data();
//            byte[] data = new byte[sa.end - sa.start - OF_DATA];
//            System.arraycopy(sa.raw, sa.start+OF_DATA, data, 0, data.length);
//            return data;
//        } finally {
//            dataItem.runLock();
//        }
//    }


    public long getXmin() {
        dataItem.rLock();
        try {
//            SubArray sa = dataItem.data();
            byte[] data1 = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(data1, OF_XMIN, OF_XMAX));
        } finally {
            dataItem.runLock();
        }
    }


    public long getRollBackPointer() {
        dataItem.rLock();
        try {
//            SubArray sa = dataItem.data();
            byte[] data1 = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(data1, OF_ROLLBACK_POINTER, OF_DATA));
        } finally {
            dataItem.runLock();
        }
    }

    public void setRollBackPointer(long pointer) {
        dataItem.rLock();
        try {
//            SubArray sa = dataItem.data();
            byte[] data1 = dataItem.data();
            System.arraycopy(Parser.unParseLong(pointer),0,data1,OF_ROLLBACK_POINTER,8);
        } finally {
            dataItem.runLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
//            SubArray sa = dataItem.data();
            byte[] data1 = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(data1, OF_XMAX, OF_DATA));
        } finally {
            dataItem.runLock();
        }
    }

    public void setXmax(long xid) {

//        SubArray sa = dataItem.data();
        byte[] data1 = dataItem.data();
        System.arraycopy(Parser.unParseLong(xid), 0, data1, OF_XMAX, 8);

    }

    public long getUid() {
        return uid;
    }
}
