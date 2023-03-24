package com.ccsu.cache;

import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Map;
@Slf4j
public abstract class AbstractCache<T> extends LinkedHashMap<Long,T> {
//    private HashMap<Long, T> cache;

    private final int capacity;

    public AbstractCache(int capacity) {
        super(capacity, 0.75F, true);
        this.capacity = capacity;
    }

    public T getFromCache(long key) throws Exception {
        T o=super.get(key);
        if(o==null){
            T t = getForCache(key);
            super.put(key,t);
            return t;
        }
        log.info("缓存命中");
        return  o;
    }



    @Override
    protected boolean removeEldestEntry(Map.Entry<Long,T> eldest) {
        return size() > capacity;
    }


    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);

    protected void close(){
        //TODO 释放资源 把资源从缓存中一一释放
    }



}
