package com.ccsu.tm;

import com.ccsu.utils.Bytes;
import com.ccsu.utils.Panic;
import com.ccsu.utils.Parser;
import com.ccsu.common.Error;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The transaction manager is used to record the status of transactions
 */
public class TransactionManagerImpl implements TransactionManager{
    public static final long SUPER_XID=0;


    // XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 9;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE   = 0;

    private static final byte FIELD_TRAN_COMMITTED = 1;

    private static final byte FIELD_TRAN_ABORTED  = 2;

    public static final String XID_SUFFIX=".xid";

    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock lock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
        checkXIDCounter();
    }

    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException);
        }
        if(fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }


    public static TransactionManagerImpl create(String path) throws Exception {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        try {
            if(!f.createNewFile()) {

            }
        } catch (Exception e) {
            throw Error.FileExistsException;
        }
        if(!f.canRead() || !f.canWrite()) {
            throw Error.FileCannotRWException;
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            throw Error.FileExistsException;
        }

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {


        }

        return new TransactionManagerImpl(raf, fc);
    }

    public static TransactionManagerImpl open(String path) throws Exception {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        if(!f.exists()) {
            throw Error.FileNotExistsException;
        }
        if(!f.canRead() || !f.canWrite()) {
            throw Error.FileCannotRWException;
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {

        }

        return new TransactionManagerImpl(raf, fc);
    }

    private void updateXID(long xid, byte status) throws Exception {
        long offset = getXidPosition(xid);

        byte[] tmp = new byte[1];
        byte[] tmp1 = Parser.unParseLong(0);
        tmp[0] = status;

        byte[] concat = Bytes.concat(tmp, tmp1);
        ByteBuffer buf = ByteBuffer.wrap(concat);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            throw Error.FileWriteException;
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            throw Error.FileWriteException;
        }
    }

    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid-1)*XID_FIELD_SIZE;
    }

    private void incrXIDCounter() {
        xidCounter ++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.unParseLong(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }



    @Override
    public long begin() {
        lock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
           lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) throws Exception {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public long getUid(long xid) throws Exception {
        ByteBuffer buffer=ByteBuffer.allocate(8);
        lock.lock();
        try {
            long position=getXidPosition(xid)+1;
            fc.position(getXidPosition(xid)+1);
            fc.read(buffer);

        }catch (IOException e){
//            throw Error.IO
            throw e;
        }finally {
            lock.unlock();
        }
        return Parser.parseLong(buffer.array());
    }

    @Override
    public void setUid(long xid,long uid) throws Exception {
        byte[] bytes = Parser.unParseLong(uid);
        lock.lock();
        try {
            long position=getXidPosition(xid)+1;
            fc.position(position);
            fc.write(ByteBuffer.wrap(bytes));
        }catch (IOException e){
            throw Error.FileWriteException;
        }finally {
            lock.unlock();
        }


    }

}
