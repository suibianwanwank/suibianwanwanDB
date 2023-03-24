package ccsu.cache;

public abstract class AbstractCacheGenerational<Long,T> {
    final private LRULinked<T> hotChain;
    final private LRULinked<ColdChainValue<T>> coldChain;

    static class ColdChainValue<T>{
        T value;
        long date;

        public ColdChainValue(T value, long date) {
            this.value = value;
            this.date = date;
        }
    }


    public AbstractCacheGenerational(int hotCapacity,int coldCapacity) {
        coldChain=new LRULinked<>(coldCapacity);
        hotChain=new LRULinked<>(hotCapacity);
    }

    public T getFromCache(long key){
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



    }

    public T get(long key){

        T value = getForCache(key);
        coldChain.put(key,new ColdChainValue<T>(value,System.currentTimeMillis()));
        return value;
    }
    public abstract T getForCache(long key);



}
