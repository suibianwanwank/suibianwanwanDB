package com.ccsu.cache.diy;

import java.util.HashMap;

public class LRULinked<T> {




    class LinkedNode{

        Long key;
        T value;

        LinkedNode nextLinkNode;
        LinkedNode proLinkNode;

        public LinkedNode(Long key, T value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "LinkedNode{" +
                    "key=" + key +
                    ", value=" + value +
                    '}';
        }
    }


    private final HashMap<Long,LinkedNode> cache=new HashMap<>();

    private final LinkedNode firstNode;
    private final LinkedNode lastNode;


    private final int maxCapacity;

    private int capacity;

    AbstractCacheGenerational generational;

    public LRULinked(int maxCapacity,AbstractCacheGenerational generational) {
        this.maxCapacity=maxCapacity;
        this.generational=generational;

        firstNode=new LinkedNode(null,null);
        lastNode=new LinkedNode(null,null);

        firstNode.nextLinkNode=lastNode;
        lastNode.proLinkNode=firstNode;
        capacity=0;
    }

    public T get(long key) {
        if(cache.containsKey(key)){
            LinkedNode linkedNode = cache.get(key);

            moveToFirst(linkedNode);

            return linkedNode.value;
        }
        return null;
    }

    public void put(long key, T value) {

         /*
            如果cache中存在该元素
            1.获取该元素将value设置为新的值
            2.把该元素移动到队首

         */
        if(cache.containsKey(key)){
            //交换value值
            LinkedNode linkedNode = cache.get(key);
            linkedNode.value=value;
            moveToFirst(linkedNode);

            return ;
        }



        /**
         * 1.默认情况：创建一个新的节点
         * 2.设置下一个节点为第一个节点，第一个节点的前置节点为该节点
         * 3.第一个节点设为自己，加入map capacity++；
         */

        LinkedNode linkedNode=new LinkedNode(key, value);

        linkedNode.nextLinkNode=firstNode.nextLinkNode;
        linkedNode.proLinkNode=firstNode;

        firstNode.nextLinkNode.proLinkNode=linkedNode;
        firstNode.nextLinkNode=linkedNode;

        cache.put(key,linkedNode);
        capacity++;
        /**
         * 如果capacity>最大容量
         *   1.capacity减一
         *   2.从map中移除最后一个节点的元素
         *   3.最后一个节点改为最后一个结点的上一个节点
         *   4.最后一个节点的下一个节点为null
          */
        if(this.capacity>maxCapacity){
            generational.releaseForCache(cache.get(key).value);
             cache.remove(lastNode.proLinkNode.key);

             lastNode.proLinkNode=lastNode.proLinkNode.proLinkNode;
             lastNode.proLinkNode.nextLinkNode=lastNode;

             capacity--;
         }


    }

    public void remove(long key) {
        LinkedNode linkedNode = cache.get(key);
        if(linkedNode==null)
            throw new RuntimeException("wrong remove action");
        linkedNode.proLinkNode.nextLinkNode=linkedNode.nextLinkNode;
        linkedNode.nextLinkNode.proLinkNode=linkedNode.proLinkNode;
        capacity--;
        cache.remove(key);
    }


    public void moveToFirst(LinkedNode linkedNode){
        linkedNode.nextLinkNode.proLinkNode=linkedNode.proLinkNode;
        linkedNode.proLinkNode.nextLinkNode=linkedNode.nextLinkNode;

        linkedNode.nextLinkNode=firstNode.nextLinkNode;
        linkedNode.proLinkNode=firstNode;

        firstNode.nextLinkNode.proLinkNode=linkedNode;

        firstNode.nextLinkNode=linkedNode;
    }

    public void close() {
        LinkedNode node = firstNode;
        while (node.nextLinkNode!=lastNode){
            node=node.nextLinkNode;
            generational.release(node.value);
        }
    }


}
