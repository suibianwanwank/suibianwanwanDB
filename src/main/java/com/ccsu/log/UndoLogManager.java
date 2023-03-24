package com.ccsu.log;

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

public class UndoLogManager {

    public static final String UNDO_LOG_SUFFIX=".udlog";

    private RandomAccessFile file;

    private FileChannel fc;

    private Lock lock;

    long spaceOffset;

    public UndoLogManager(RandomAccessFile file,FileChannel fc){
        this.file=file;
        this.fc=fc;
        lock=new ReentrantLock();
    }

    public static UndoLogManager create(String path){
        File f = new File(path+UNDO_LOG_SUFFIX);
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

        ByteBuffer buf = ByteBuffer.wrap(Parser.unParseLong(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

        UndoLogManager undoLogManager = new UndoLogManager(raf, fc);
        undoLogManager.spaceOffset=8;
        return undoLogManager;


    }


    public static UndoLogManager open(String path) {
        File f = new File(path+UNDO_LOG_SUFFIX);
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


        UndoLogManager lg = new UndoLogManager(raf, fc);
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
            Panic.panic(Error.UndoBadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(8);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }

        //提取日志的数目
        //日志数目的本质是：每条日志的长度应是固定的
        this.spaceOffset=Parser.parseLong(raw.array());

//        int xChecksum = Parser.parseInt(raw.array());
//        this.fileSize = size;
//        this.xChecksum = xChecksum;
//
//        checkAndRemoveTail();
    }

    public  UndoLog getUndoLog(long uid){
        ByteBuffer rawLength = ByteBuffer.allocate(4);
        ByteBuffer buf=null;
        lock.lock();
        try{
            fc.position(uid+25);
            fc.read(rawLength);
            buf = ByteBuffer.allocate(Parser.parseInt(rawLength.array())+29);
            fc.position(uid);
            fc.read(buf);

        }catch (IOException e){
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
        assert buf != null;
        return UndoLog.parseUndoLog(buf.array());

    }


    public long log(byte type,long xid, long xidUid,long dataUid,byte[] raw) throws Exception {
        UndoLog log=new UndoLog(type,xid,xidUid,dataUid,raw);
        byte[] bytes = UndoLog.unParseUndoLog(log);
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        //TODO 锁的范围是否可以变小
        lock.lock();
        long t=this.spaceOffset;

        try {
            fc.position(spaceOffset);
            fc.write(wrap);
            this.spaceOffset=t+bytes.length;
        }catch (Exception e){
            throw Error.WriteFailException;
        }finally {
            lock.unlock();
        }

        return t;
    }

}
