package com.ccsu.log;

import com.ccsu.common.Error;
import com.ccsu.page.Page;
import com.ccsu.page.PageManager;
import com.ccsu.utils.Bytes;
import com.ccsu.utils.Panic;
import com.ccsu.utils.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RedoLogManager {


    private static final int OF_SIZE =0 ;
    private static final int OF_CHECK_SUM = OF_SIZE+4;
    private static final int OF_DATA = OF_CHECK_SUM+4;


    private final static String REDO_LOG_SUFFIX=".rdLog";
    private final static int SEED=13331;

    private RandomAccessFile file;

    private FileChannel fc;

    private Lock lock;

    private int xChecksum;
    private long fileSize;
    private int position;



    public RedoLogManager(RandomAccessFile raf, FileChannel fc) {
        this.file=raf;
        this.fc=fc;
        lock = new ReentrantLock();
    }


    public static RedoLogManager create(String path){
        File f = new File(path+REDO_LOG_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileExistsException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(Parser.unParseInt(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new RedoLogManager(raf,fc);


    }


    public static RedoLogManager open(String path) {
        File f = new File(path+REDO_LOG_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        RedoLogManager lg = new RedoLogManager(raf, fc);
        lg.init();

        return lg;
    }



    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    private byte[] internNext() {
        if(position + OF_DATA >= fileSize) {
            return null;
        }
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if(position + size + OF_DATA > fileSize) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {

            fc.position(position);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECK_SUM, OF_DATA));
        if(checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;

    }

    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        while(true) {
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    public void truncate(long x) throws Exception {
        lock.lock();
        try {

            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    public void rewind() {
        position = 4;
    }
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {RedoLog.TYPE_INSERT};
        byte[] xidRaw = Parser.unParseLong(xid);
        byte[] pgnoRaw = Parser.unParseInt(pg.getPageNumber());
        byte[] offsetRaw = Parser.unParseInt(PageManager.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }


    public static byte[] updateLog(long xid, int pgno, int offset,byte[] oldRaw, byte[] raw) {
        byte[] logTypeRaw = {RedoLog.TYPE_UPDATE};
        byte[] xidRaw = Parser.unParseLong(xid);
        byte[] pgnoRaw = Parser.unParseInt(pgno);
        byte[] offsetRaw = Parser.unParseInt(offset);
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw,oldRaw, raw);
    }

    private byte[] wrapLog(byte[] log){
        byte[] checksum = Parser.unParseInt(calChecksum(0, log));
        byte[] size=Parser.unParseInt(log.length);
        return Bytes.concat(size,checksum,log);
    }

    public void log(byte[] log) {
        byte[] wrapLog = wrapLog(log);
        ByteBuffer buf = ByteBuffer.wrap(wrapLog);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(wrapLog);
    }

    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.unParseInt(xChecksum)));
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

}
