package com.ccsu.dm;


import com.ccsu.page.Page;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.utils.Bytes;
import com.ccsu.utils.Parser;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 *
 */
@Slf4j
public class DataItemImpl implements DataItem{

    private  static final int VALID_OFFSET=0;

    private static final int SIZE_OFFSET=1;

    private static  final int DATA_OFFSET = SIZE_OFFSET+4;

    private byte[] raw;

    private byte[] data;

    private Lock rLock;
    private Lock wLock;

    private DataManagerImpl dm;
    private long uid;

    private Page page;

    private DataItemImpl(byte[] raw){
        this.raw=raw;
        this.data=Arrays.copyOfRange(raw,DATA_OFFSET,raw.length);
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
    }

    public DataItemImpl(byte[] raw, long uid,Page page,DataManagerImpl dataManager) {
        this.raw=raw;
        this.data=Arrays.copyOfRange(raw,DATA_OFFSET,raw.length);
        this.uid=uid;
        this.page=page;
        this.dm=dataManager;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();

    }

    @Override
    public byte[] data() {
        return data;
    }
    @Override
    public byte[] getRaw() {
        return raw;
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void runLock() {
        rLock.unlock();
    }


    @Override
    public Page page() {
        return page;
    }


    @Override
    public long getUid() {
        return uid;
    }





    @Override
    public void release() throws Exception {
        dm.update(TransactionManagerImpl.SUPER_XID,uid,data);
    }

    public static DataItem parseDataItem(Page pg, int offset, DataManagerImpl dataManager) {
        byte []raw=pg.getData();
        int size = Parser.parseInt(Arrays.copyOfRange(raw, offset+SIZE_OFFSET, offset+DATA_OFFSET));
        int len = size + DATA_OFFSET;
        long uid = Types.addressToUid(pg.getPageNumber(), offset);

        return new DataItemImpl(Arrays.copyOfRange(raw,offset,offset+len),uid,pg,dataManager);
    }

    public static DataItem transBaseDataItem(byte[] raw){
        byte[] cooked = wrapDataItemRaw(raw);
        return new DataItemImpl(cooked);
    }

    public static byte[] wrapDataItemRaw(byte[] raw) {

        byte[] valid = new byte[]{1};
        byte[] size = Parser.unParseInt(raw.length);
        return Bytes.concat(valid, size, raw);

    }

    public static byte[] deleteWrapDataItemRaw(byte[] raw) {
        //todo 有待观察
        raw[0]=0;
        return raw;
    }

    public boolean isValid() {
        return raw[0] == (byte)1;
    }
}
