package com.ccsu.cache.diy;

import com.ccsu.page.Page;

import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Cache replacement for pages
 * The LRU algorithm is optimized using hot and cold chains
 * @param <T>
 */
public abstract class AbstractCacheGenerational<T> {
    final private LRULinked<T> hotChain;
    final private LRULinked<ColdChainValue<T>> coldChain;
    private Lock lock;

    static class ColdChainValue<T>{
        T value;
        long date;

        public ColdChainValue(T value, long date) {
            this.value = value;
            this.date = date;
        }
    }


    public AbstractCacheGenerational(int hotCapacity,int coldCapacity) {
        coldChain=new LRULinked<>(coldCapacity,this);
        hotChain=new LRULinked<>(hotCapacity,this);
        lock=new ReentrantLock();
    }

    public T getFromCache(long key) throws Exception {
        lock.lock();
        try{
            if(hotChain.get(key)!=null){
                return hotChain.get(key);
            }

            if(coldChain.get(key)!=null){
                ColdChainValue<T> coldChainValue = coldChain.get(key);
                if(System.currentTimeMillis()-coldChainValue.date>1000){

                    //从冷链中移除
                    coldChain.remove(key);
                    //插入热链
                    hotChain.put(key,coldChainValue.value);

                }
                return coldChainValue.value;
            }
            return get(key);
        }finally {
            lock.unlock();
        }





    }

    public T get(long key) throws Exception {

        T value = getForCache(key);
        coldChain.put(key,new ColdChainValue<T>(value,System.currentTimeMillis()));
        return value;
    }

    protected void release(Object obj){
        if(obj instanceof Page){
            releaseForCache((T) obj);
        }else if(obj instanceof ColdChainValue<?> obj1){
            releaseForCache((T)obj1.value);
        }
    }


    /**
     * Turn off caching to free up all resources
     */
    protected void close() {
        lock.lock();
        try {
            hotChain.close();
            coldChain.close();

        } finally {
            lock.unlock();
        }
    }

    //Get the resource into the cache
    protected abstract T getForCache(long key) throws Exception;
    //Release resources from the cache
    protected abstract void releaseForCache(T obj);




}
