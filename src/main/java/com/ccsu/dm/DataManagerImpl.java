package com.ccsu.dm;

import com.ccsu.cache.AbstractCache;
import com.ccsu.cache.PageCache;
import com.ccsu.cache.PageCacheImpl;
import com.ccsu.common.Error;
import com.ccsu.log.Recover;
import com.ccsu.log.RedoLog;
import com.ccsu.log.RedoLogManager;
import com.ccsu.log.UndoLog;
import com.ccsu.page.Page;
import com.ccsu.page.PageManager;
import com.ccsu.page.PageOne;
import com.ccsu.pgindex.PageIndex;
import com.ccsu.pgindex.PageInfo;
import com.ccsu.tm.TransactionManager;
import com.ccsu.utils.Panic;
import com.ccsu.utils.Parser;
import com.ccsu.utils.SubArray;
import com.ccsu.vm.Entry;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Slf4j
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{

    PageCache pc;

    Page pageOne;

    PageIndex pIndex;

    RedoLogManager redoLogManager;

    public DataManagerImpl(PageCache pc,RedoLogManager redoLogManager) {
        super(0);
        this.pc = pc;
        this.redoLogManager=redoLogManager;
        this.pIndex = new PageIndex();
    }


    public static DataManager create(String path, long mem,RedoLogManager redoLogManager) {
        PageCache pc = PageCacheImpl.create(path, mem);
//        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc,redoLogManager);

        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, TransactionManager tm, long mem, RedoLogManager redoLogManager) throws Exception {
        PageCache pc = PageCacheImpl.open(path, mem);
        DataManagerImpl dm = new DataManagerImpl(pc,redoLogManager);

        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, redoLogManager, pc);
        }

        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }

    @Override
    public void close() {
        super.close();
        PageOne.close(pageOne);
        pageOne.release();
        pc.close();
    }

    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
        }
        return PageOne.check(pageOne);
    }

    private void initPageOne() {
//        log.info("初始化第一页");
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            log.error("getPage失败");
        }
        PageOne.setVcOpen(pageOne);
        pc.flushPage(pageOne);

    }

    @Override
    public DataItem read(long uid) throws Exception {
        //        byte[] bytes = Arrays.copyOfRange(dataItem.data().raw, dataItem.data().start, dataItem.data().end);
        DataItemImpl item = (DataItemImpl) super.getFromCache(uid);
        if(!item.isValid()){
            item.release();
            return null;
        }
        return item;
    }

    /**
     * 插入数据
     * @param xid 处理事件的事务
     * @param data 插入的数据
     * @return 返回uid
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {

        byte[] raw = DataItemImpl.wrapDataItemRaw(data);
        int spaceSize=raw.length;

        PageInfo pi=null;
        for(int i = 0; i < 5; i ++) {

            pi=pIndex.select(spaceSize);
            if(pi==null){
                int newPgno = pc.newPage(PageManager.initRaw());
                pIndex.add(newPgno, PageManager.MAX_FREE_SPACE);
            }else{
                break;
            }
        }




//        DataItem dataItem = getFromCache(pi.pgNo);
        Page pg = null;
        int freeSpace = 0;
        try{
            pg = pc.getPage(pi.pgno);
            byte[] log = RedoLogManager.insertLog(xid, pg, raw);


            //记录日志
            redoLogManager.log(log);

            int offset = PageManager.insert(pg, raw);
            return Types.addressToUid(pi.pgno, offset);

        }
        finally {
            if(pg != null) {
//                log.info("把page刷新回磁盘，作为测试使用，以后删除");
                pc.flushPage(pg);
                pIndex.add(pi.pgno, PageManager.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }



    }

    @Override
    public void update(long xid,long uid, byte[] data) throws Exception {
        byte[] raw = DataItemImpl.wrapDataItemRaw(data);
        int pgno= (int) (uid>>32);
        int offset= (int) (uid& 0x00000000ffffffffL);
        DataItem oldDataItem = read(uid);
        byte[] oldRaw = Arrays.copyOfRange(oldDataItem.getRaw(), 0, oldDataItem.getRaw().length);
        if(oldRaw.length!=raw.length){
            Panic.panic(Error.WrongUpdateException);
        }
        byte[] log = RedoLogManager.updateLog(xid, pgno, offset, oldRaw, raw);
        redoLogManager.log(log);

        Page pg = null;
        pg = pc.getPage(pgno);

        PageManager.insert(pg,raw,offset);

    }


    @Override
    protected DataItem getForCache(long uid) throws Exception {
        int pgNo= (int) (uid>>32);
//        log.info("pgNo:{}",pgNo);
        int offset= (int) (uid &0x00000000ffffffffL);
//        log.info("offset:{}",offset);
        Page pg=pc.getPage(pgNo);
        return DataItemImpl.parseDataItem(pg,offset,this);

    }

    /**
     * 当资源被驱逐时的写回行为
     *
     * @param obj
     */
    @Override
    protected void releaseForCache(DataItem obj) {

    }

    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
//                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageManager.getFreeSpace(pg));
            pg.release();
        }
    }



}
