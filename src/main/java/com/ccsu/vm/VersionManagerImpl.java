package com.ccsu.vm;

import com.ccsu.cache.AbstractCache;
import com.ccsu.common.Error;
import com.ccsu.dm.DataItem;
import com.ccsu.dm.DataManager;
import com.ccsu.log.UndoLog;
import com.ccsu.log.UndoLogManager;
import com.ccsu.tm.Transaction;
import com.ccsu.tm.TransactionManager;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.utils.Bytes;
import com.ccsu.utils.Parser;
import com.ccsu.utils.SubArray;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;

    LockManager lockManager;

    UndoLogManager undoLogManager;

    public VersionManagerImpl(TransactionManager tm, DataManager dm,UndoLogManager logManager) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        this.undoLogManager=logManager;
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        lockManager=new LockManager();
    }
    //快照
    @Override
    public byte[] read(long xid, long uid,boolean isSnapshot) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.getFromCache(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(isSnapshot){
                //返回快照读
                return snapshotRead(xid,uid,t);
            }else{
                //返回当前读
                if(entry.getXmax()!=0){
                    return null;
                }
                return entry.data();
            }
        } finally {
            entry.release();
        }
    }

    public byte[] snapshotRead(long xid,long uid, Transaction t) throws Exception {
        Entry entry = null;
        try{
            entry = super.getFromCache(uid);
        }catch (Exception e){
            if(e==Error.NullEntryException){
                return null;
            }
        }
        if(Visibility.isVisible(tm, t, entry)) {
            return entry.data();
        }
        if(entry.getRollBackPointer()==0){
            return null;
        }
        UndoLog undoLog = undoLogManager.getUndoLog(entry.getRollBackPointer());
        while(true){
            //判断是否可见
            entry=Entry.loadEntry(Arrays.copyOfRange(undoLog.getData(),8,undoLog.getData().length));
            if(Visibility.isVisible(tm, t, entry)){
                return entry.data();
            }else{
                if(undoLog.getDataUid()==0)return null;
                undoLog=undoLogManager.getUndoLog(undoLog.getDataUid());
                if(undoLog.getType()==UndoLog.TYPE_INSERT){
                    return null;
                }
            }
        }



    }




    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data,0);


        long insert = dm.insert(xid, raw);
        long log = undoLogManager.log(UndoLog.TYPE_INSERT,xid, tm.getUid(xid),0,Parser.unParseLong(insert));
        tm.setUid(xid,log);
        return insert;

    }
    @Override
    public void update(long xid,long uid,byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        try{
            lockManager.add(xid,uid);
        }catch (Exception e){
            System.out.println("执行回滚");
            internAbort(xid,true);
            System.out.println("回滚");
        }

        Entry entry = null;
        try {
            entry = super.getFromCache(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return ;
            } else {
                throw e;
            }
        }
        //在这里用log先记录然后返回一个地址加入其中

        byte[] concat = Bytes.concat(Parser.unParseLong(uid),entry.getEntry());
        long log = undoLogManager.log(UndoLog.TYPE_UPDATE,xid, tm.getUid(xid),entry.getRollBackPointer(),concat);
        entry.setRollBackPointer(log);
        entry.release();
        tm.setUid(xid,log);

        byte[] raw = Entry.wrapEntryRaw(xid, data,log);

        dm.update(xid,uid,raw);

    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        //1.从活跃事务表中获取当前事务
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        //查看当前事务是否出现异常
        if(t.err != null) {
            throw t.err;
        }
        //根据uid读取数据
        Entry entry = null;

        try {
            lockManager.add(xid, uid);
        } catch(Exception e) {
            System.out.println("发生死锁");
            internAbort(xid,true);
        }

        try {
            entry = super.getFromCache(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }

        try {
            //判断对当前事务是否可见，如果不可见，不能删除
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            //如果可见，判断是否回造成死锁，造成了则回滚


            //如果已经被当前事务删除了，返回删除失败
            if(entry.getXmax() == xid) {
                return false;
            }

            byte[] data = entry.getEntry();
            long log = undoLogManager.log(UndoLog.TYPE_DELETE, xid, tm.getUid(xid), entry.getRollBackPointer(), Bytes.concat(Parser.unParseLong(uid),data));
            tm.setUid(xid,log);
            entry.setRollBackPointer(log);
            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
//            Panic.panic(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();
        lockManager.remove(xid);
//        lt.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) throws Exception {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
//        if(!autoAborted) {
//            activeTransaction.remove(xid);
//        }
        lock.unlock();

//        if(t.autoAborted) return;
        rollBack(xid);
        tm.abort(xid);
        lockManager.remove(xid);

    }

    private void rollBack(long xid) throws Exception{
        //1.获取xid中的第一个

        long uid = tm.getUid(xid);

        while(uid!=0){
            UndoLog undoLog = undoLogManager.getUndoLog(uid);
            if(undoLog.getType()==UndoLog.TYPE_INSERT){
                doInsertRollback(undoLog);
            }else if(undoLog.getType()==UndoLog.TYPE_UPDATE){
                doUpdateRollback(undoLog);
            } else if (undoLog.getType()==UndoLog.TYPE_DELETE) {
                doDeleteRollback(undoLog);
            }
            uid=undoLog.getXidUid();
        }



//        undoLogManager.get
    }

    private void doUpdateRollback(UndoLog log) throws Exception {
        //回滚
        byte[] data = log.getData();
        long uid = Parser.parseLong(Arrays.copyOfRange(data, 0, 8));
        dm.update(TransactionManagerImpl.SUPER_XID,uid,Arrays.copyOfRange(data,8,data.length));
    }


    private void doDeleteRollback(UndoLog log) throws Exception {
        //回滚
        byte[] data = log.getData();
        long uid = Parser.parseLong(Arrays.copyOfRange(data, 0, 8));
        Entry entry=null;
        try {
            entry=super.getFromCache(uid);
        }catch (Exception e){

        }
        assert entry != null;
        entry.setXmax(0);
    }
    private void doInsertRollback(UndoLog log) throws Exception {
        byte[] data = log.getData();
        long uid = Parser.parseLong(Arrays.copyOfRange(data, 0, 8));
        DataItem dataItem = dm.read(uid);
        byte[] raw = dataItem.getRaw();
        System.arraycopy(new byte[]{0},0,raw,0,1);

    }


    @Override
    protected Entry getForCache(long uid) throws Exception {

        return Entry.loadEntry(this, uid);
    }

    @Override
    protected void releaseForCache(Entry obj) {

    }


}
