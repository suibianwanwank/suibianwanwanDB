package com.ccsu.cache;

import com.ccsu.cache.diy.AbstractCacheGenerational;
import com.ccsu.common.Error;
import com.ccsu.page.Page;
import com.ccsu.page.PageImpl;
import com.ccsu.utils.Panic;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.ccsu.page.PageManager.PAGE_SIZE;
@Slf4j
public class PageCacheImpl extends AbstractCacheGenerational<Page> implements PageCache{

//    private static final int MEM_MIN_LIM = 10;

    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;

    private FileChannel fc;

    private Lock fileLock;

    private AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int capacity) {
        super(capacity,capacity);
//        if(capacity < MEM_MIN_LIM) {
//            Panic.panic(Error.MemTooSmallException);
//        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
//            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);

    }




    public static PageCacheImpl open(String path,long memory){
        File f=new File(path+DB_SUFFIX);
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return new PageCacheImpl(raf, fc, 100*1024*1024);
    }

    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
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
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData,this);
        flush(pg);
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return getFromCache(pgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        releaseForCache(page);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        System.out.println(pgno);
        long offset = PageCacheImpl.pageOffset(pgno);
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buf.array(),this);

    }


    private static long pageOffset(int pgno) {
        return (pgno-1) * PAGE_SIZE;
    }

    /**
     *
     * @param pg
     */

    public void flushPage(Page pg) {
        flush(pg);
    }

    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    private void flush(Page pg) {
        int pgNo = pg.getPageNumber();
        long offset = pageOffset(pgNo);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch(IOException e) {
            e.printStackTrace();
//            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }


    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }
}
