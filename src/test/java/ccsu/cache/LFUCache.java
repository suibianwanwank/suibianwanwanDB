package ccsu.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class LFUCache<T> extends LinkedHashMap<Long,T> {

    private int capacity;

    public LFUCache(int capacity) {
        super(capacity, 0.75F, true);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<Long,T> eldest) {
        return size() > capacity;
    }

    public T get(long n){
        return super.get(n);
    }

    public T put(long n,T value){
        return super.put(n,value);
    }
}
