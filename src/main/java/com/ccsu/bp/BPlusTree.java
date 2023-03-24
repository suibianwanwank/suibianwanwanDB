package com.ccsu.bp;

import com.ccsu.dm.DataItem;
import com.ccsu.dm.DataManager;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.tp.Type;
import com.ccsu.utils.Parser;
import com.ccsu.utils.SubArray;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BPlusTree {

    DataManager dm;

    long bootUid;

    DataItem bootDataItem;

    Lock bootLock;

    String typeName;

    /**
     * Create a B+ tree
     * @param dm
     * @return
     * @throws Exception
     */
    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.unParseLong(rootUid));
    }

    /**
     * load a B+ tree
     * @param uid
     * @param dm
     * @param typeName
     * @return
     * @throws Exception
     */
    public static BPlusTree load(long uid, DataManager dm,String typeName) throws Exception {
        DataItem bootDataItem = dm.read(uid);
        BPlusTree t = new BPlusTree();
        t.bootUid=uid;
        t.dm=dm;
        t.bootDataItem=bootDataItem;
        t.bootLock=new ReentrantLock();
        t.typeName=typeName;
        return t;
    }

    /**
     * Externally exposed insert method
     * @param key
     * @param uid
     * @throws Exception
     */
    public void insert(Type key, long uid) throws Exception {
        long rootUid = rootUid();
        insert(rootUid,uid,key);

    }

    /**
     * insert key
     * @param nodeUid
     * @param uid
     * @param key
     * @throws Exception
     */
    private void insert(long nodeUid, long uid, Type key) throws Exception {

        Node node = Node.loadNode(this, nodeUid,typeName);
        Node.Res res = node.insert(uid, key);
        if(res!=null){

            long fatherNode = node.createFatherNode(res.uid,res.key);
            bootDataItem.lock();
            try{
                byte[] data = bootDataItem.data();
                System.arraycopy(Parser.unParseLong(fatherNode),0,data,0,8);
                bootDataItem.release();
            }finally {
                bootDataItem.unlock();
            }
        }

    }


    public List<Long> searchRange(Type leftKey, Type rightKey) throws Exception {
        long rootUid = rootUid();
        Node node=Node.loadNode(this,rootUid,typeName);
        return node.searchRange(leftKey, rightKey);
    }



    public long rootUid() {
        bootLock.lock();
        try {
            byte[] data = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(data, 0, 8));
        } finally {
            bootLock.unlock();
        }

    }

}
