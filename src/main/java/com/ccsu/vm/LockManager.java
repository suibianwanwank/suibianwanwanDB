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
    //xid拥有的多个uid资源
    private Map<Long, List<Long>> x3u;
    //uid正被xid所持有
    private Map<Long,Long> u1x;
    //正在等待uid的多个xid
    private Map<Long,List<Long>> waitu3x;
    //xid正在等待的uid
//    private Map<Long, Long> wait;

    private Condition condition ;

    private Lock lock;

    public LockManager(){
        x3u=new HashMap<>();
        u1x=new HashMap<>();
        waitu3x=new HashMap<>();
//        wait=new HashMap<>();
        lock=new ReentrantLock();
        condition= lock.newCondition();
    }

    public void add(long xid,long uid) throws Exception{
        lock.lock();
        System.out.println(xid+"尝试获取获取"+uid);
        try {
            //1.如果已经获得该资源
            if(x3u.containsKey(xid)&&x3u.get(xid).contains(uid)){
                return ;
            }
            //2.如果该线程未被其他线程持有
            if(!u1x.containsKey(uid)){
                //让他持有这个对象
                if(!x3u.containsKey(xid)){
                    x3u.put(xid,new ArrayList<Long>());
                }
                u1x.put(uid,xid);
                x3u.get(xid).add(uid);
                return ;
            }

            //3.如果该线程正被其他线程持有
                //进入定时等待，等待唤醒,如果超时回滚线程
                //如果没有超时被唤醒，获取该资源并继续执行
            //首先判断等待是否会产生死锁，如果会

            //如果不会进入等待队列
            if(!waitu3x.containsKey(uid)){
                waitu3x.put(uid,new ArrayList<>());
            }

            waitu3x.get(uid).add(xid);
//            wait.put(xid,uid);
            if(willDeadLock(xid)){
                //产生死锁
                System.out.println("发生死锁问题，抛出异常");
                throw Error.DeadLockException;
            }

            System.out.println(xid+"进入"+uid+"的等待队列");
            condition.await();
            waitu3x.get(uid).remove(xid);


        } catch (InterruptedException e) {
            waitu3x.get(uid).remove(xid);

            throw e;
        } finally {

            lock.unlock();
        }

    }


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
