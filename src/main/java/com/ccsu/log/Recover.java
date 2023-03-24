package com.ccsu.log;

import com.ccsu.cache.PageCache;
import com.ccsu.cache.PageCacheImpl;
import com.ccsu.dm.DataItemImpl;
import com.ccsu.page.Page;
import com.ccsu.page.PageManager;
import com.ccsu.tm.TransactionManager;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Recover {
    RedoLogManager redoLogManager;
    public static void recover(TransactionManager tm, RedoLogManager redoLogManager, PageCache pc) throws Exception {
        //1.先遍历log
        redoLogManager.rewind();
        int maxPgno = 0;
        List<RedoLog> redoLogList = new ArrayList<>();
        while(true){
            byte[] log = redoLogManager.next();
            if(log==null) break;

            RedoLog reLog = RedoLog.parseRedoLog(log);
            redoLogList.add(reLog);

            if(reLog.getPgno()>maxPgno){
                maxPgno= reLog.getPgno();
            }
        }

        pc.truncateByBgno(maxPgno);

        for (RedoLog redoLog : redoLogList) {
            doRedoLog(tm,pc,redoLog);
        }
        //      如果已经提交，覆盖
        //      如果未提交，回滚

        //3. 从日志的什么位置开始搜索，在每个页记录最新的position，page刷回的时候写入最早的
    }

    public static void doRedoLog(TransactionManager tm,PageCache pc,RedoLog redoLog) throws Exception {
        Page page=pc.getPage(redoLog.getPgno());
        switch (redoLog.getType()){
            case RedoLog.TYPE_INSERT -> {
                if(tm.isCommitted(redoLog.getXid())){
                    PageManager.recoverInsert(page,redoLog.getOffset(),redoLog.getRaw());
                }else{
                    //优化为观察page的offset
                    byte[] raw2 = DataItemImpl.deleteWrapDataItemRaw(redoLog.getRaw());
                    PageManager.recoverInsert(page,redoLog.getOffset(),raw2);
                }
            }
            case RedoLog.TYPE_UPDATE -> {
                if(tm.isCommitted(redoLog.getXid())){
                    PageManager.recoverUpdate(page,redoLog.getOffset(),redoLog.getRaw());
                }else{
                    PageManager.recoverUpdate(page,redoLog.getOffset(),redoLog.getOldRaw());
                }
            }
        };
        if(tm.isActive(redoLog.getXid())){
            tm.abort(redoLog.getXid());
        }
    }




}
