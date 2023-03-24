package com.ccsu.vm;

import com.ccsu.common.Error;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockManager {
    //Multiple UID resources owned by xid
    private Map<Long, List<Long>> x3u;
    //UID is being held by XID
    private Map<Long,Long> u1x;
    //Multiple xids waiting for UIDs
    private Map<Long,List<Long>> waitu3x;


    private Condition condition ;

    private Lock lock;

    public LockManager(){
        x3u=new HashMap<>();
        u1x=new HashMap<>();
        waitu3x=new HashMap<>();

        lock=new ReentrantLock();
        condition= lock.newCondition();
    }

    public void add(long xid,long uid) throws Exception{
        lock.lock();
        try {
            //1.If the thread already holds the lock
            if(x3u.containsKey(xid)&&x3u.get(xid).contains(uid)){
                return ;
            }
            //2.If no thread is already holding the lock
            if(!u1x.containsKey(uid)){

                if(!x3u.containsKey(xid)){
                    x3u.put(xid,new ArrayList<Long>());
                }
                u1x.put(uid,xid);
                x3u.get(xid).add(uid);
                return ;
            }
            //3.
            if(!waitu3x.containsKey(uid)){
                waitu3x.put(uid,new ArrayList<>());
            }

            waitu3x.get(uid).add(xid);
            //Determine if a deadlock will occur
            if(willDeadLock(xid)){
                throw Error.DeadLockException;
            }

            condition.await();
            waitu3x.get(uid).remove(xid);


        } catch (InterruptedException e) {
            waitu3x.get(uid).remove(xid);

            throw e;
        } finally {

            lock.unlock();
        }

    }

    /**
     * Release all resources held by the transaction
     * @param xid
     */
    public void remove(long xid){
        lock.lock();
        try{
            if(!x3u.containsKey(xid)){
                return ;
            }
            List<Long> xUids = x3u.get(xid);
            for (Long xUid : xUids) {
                u1x.remove(xUid);
                if(waitu3x.containsKey(xUid)&&waitu3x.get(xUid).size()!=0){
                    condition.signal();
                }
            }
            x3u.remove(xid);
        } finally {
          lock.unlock();
        }

    }

    /**
     * DFS traversal determines deadlocks
     * @param xid
     * @return
     */
    private boolean willDeadLock(long xid) {

        Map<Long,Boolean> map=new HashMap<>();

        return dfs(map,xid);
    }

    private boolean dfs(Map<Long,Boolean> map,long xid){
        List<Long> list = x3u.get(xid);
        //遍历xid所拥有的资源
        if(list==null)return false;
        for (Long uid1 : list) {
            //判断这些资源是否存在等待队列且是否为空
            if(waitu3x.get(uid1)!=null&&waitu3x.get(uid1).size()!=0){
                //遍历等待队列，map中加入队列
                for (Long aXid : waitu3x.get(uid1)) {
//                    Long xid1 = u1x.get(aUid);
                    if(map.containsKey(aXid)){
                        return true;
                    }
                    map.put(aXid,true);
                    if(dfs(map,aXid)){
                        return true;
                    }
                }
            }
        }
        return false;

    }


}
