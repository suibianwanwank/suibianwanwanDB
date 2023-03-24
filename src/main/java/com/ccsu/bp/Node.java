package com.ccsu.bp;

import com.ccsu.dm.DataItem;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.tp.Type;
import com.ccsu.tp.TypeFactory;
import com.ccsu.utils.Bytes;
import com.ccsu.utils.Panic;
import com.ccsu.utils.Parser;
import com.ccsu.utils.SubArray;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 存储结构：/ isLeaf / /keyNumbers/ / beforeNode / /AfterNode/ /son/ /key/ /son/ /key/...
 * 1           4             4            8             8        8    8   8    8
 */
@Slf4j
public class Node {

    static final int IS_LEAF_OFFSET = 0;

    static final int KEYS_NUM_OFFSET = IS_LEAF_OFFSET + 1;

    static final int BEFORE_NODE_OFFSET = KEYS_NUM_OFFSET + 4;

    static final int AFTER_NODE_OFFSET = BEFORE_NODE_OFFSET + 8;

    static final int NODE_HEADER_SIZE = AFTER_NODE_OFFSET + 8;

    static final int BALANCE_NUMBER = 3;

    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2);

    BPlusTree tree;

    DataItem dataItem;

    byte[] raw;

    long uid;

    String typeName;

    static Node loadNode(BPlusTree bTree, long uid, String typeName) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.typeName = typeName;
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;

        return n;
    }

    /**
     * Insert data recursively
     * @param uid
     * @param key
     * @return
     * @throws Exception
     */
    public Res insert(long uid, Type key) throws Exception {

        dataItem.lock();
        try {
            //Find the number of the first number in the node that is greater than key
            long leftBound = findLeafBound(key);


            if (!getRawIfLeaf(raw)) {

                if (leftBound == getRawNoKeys(raw)) {
                    setRawKey(raw, (int) leftBound-1, key);
                    dataItem.release();
                    leftBound = getRawNoKeys(raw) - 1;
                }


                Node node = Node.loadNode(tree, getRawKthSon(raw, (int) leftBound), typeName);
                Res res = node.insert(uid, key);


                if (res == null) return null;
                shiftRawKeyAndSon(raw, (int) leftBound, res.key, res.uid);


            } else {
                shiftRawKeyAndSon(raw, (int) leftBound, key, uid);
            }

            dataItem.release();
            //After inserting the data, check if you need to split the node
            if (checkNum()) return subNode();

            return null;
        } catch (Exception e){
            Panic.panic(e);
        }finally {
            dataItem.unlock();
        }

        return null;
    }


    public SearchRes find(Type key) throws Exception {
        dataItem.rLock();
        try{
            SearchRes res = new SearchRes();
            long leftBound = findLeafBound(key);

            if (!getRawIfLeaf(raw)) {
                long son = getRawKthSon(raw, (int) leftBound);
                Node node = Node.loadNode(tree, son, typeName);
                return node.find(key);

            } else {
                res.uid = uid;
                res.leftBound = leftBound;
//            log.info("uid is {}",uid);
//            log.info("leftBound is {}",leftBound);
                return res;
            }
        }finally {
            dataItem.runLock();
        }


    }

    public byte[] getRaw() {
        return raw;
    }

    private class SearchRes {
        long uid;
        long leftBound;
    }

    public List<Long> searchRange(Type left, Type right) throws Exception {
        List<Long> uids = new ArrayList<>();
        SearchRes res = null;
        try {
            dataItem.rLock();
            if (left == null) res = findMinData();
            else res = find(left);

        } finally {
            dataItem.runLock();
        }
        long nextUid = res.uid;


        while (true) {
            Node node1 = Node.loadNode(tree, nextUid, typeName);
            node1.dataItem.rLock();
            try {

                byte[] raw2 = node1.getRaw();

                for (int i = (int) res.leftBound; i < getRawNoKeys(raw2); i++) {
                    if ((left == null || left.compareTo(getRawKthKey(raw2, i)) <= 0) && (right == null || right.compareTo(getRawKthKey(raw2, i)) >= 0)) {
                        uids.add(getRawKthSon(raw2, i));
                    } else {
                        return uids;
                    }
                }
                nextUid = getRawAfter(raw2);
                res.leftBound = 0;
                if (nextUid == 0) {

                    return uids;
                }
            } finally {
                node1.dataItem.rLock();
            }


        }

    }

    private SearchRes findMinData() throws Exception {
        SearchRes res = new SearchRes();
        long leftBound = 0;

        if (!getRawIfLeaf(raw)) {
            long son;
            dataItem.rLock();
            try{
                son= getRawKthSon(raw, (int) leftBound);
            }finally {
                dataItem.runLock();
            }
            Node node = Node.loadNode(tree, son, typeName);
            return node.findMinData();
        } else {
            res.uid = uid;
            res.leftBound = leftBound;
            return res;
        }


    }


    private long getRawKthSon(byte[] raw, int pos) {
        int offset = NODE_HEADER_SIZE + pos * (8 * 2);
        return Parser.parseLong(Arrays.copyOfRange(raw, offset, offset + 8));
    }

    private Res subNode() throws Exception {
        //主打的就是切割
        Res broNode = createBroNode();
        return broNode;
    }

    private Res createBroNode() throws Exception {
//        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        dataItem.lock();
        try{
            byte[] nodeRaw=new byte[NODE_SIZE];
            setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
            setRawNoKeys(nodeRaw, BALANCE_NUMBER);
            setRawBefore(nodeRaw, getRawBefore(raw));
            setRawAfter(nodeRaw, uid);
            copyRawFromPos(raw, nodeRaw, BALANCE_NUMBER);
            long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw);

            //还要去加载上一个节点让他的after为该节点
            //fixme 可能会出现问题
            long rawBefore = getRawBefore(raw);

            if(rawBefore!=0){
                Node node=loadNode(tree,rawBefore,typeName);
                setRawAfter(node.getRaw(),son);
                node.dataItem.release();
            }


            System.arraycopy(raw,  NODE_HEADER_SIZE + (BALANCE_NUMBER) * 2 * 8, raw,  NODE_HEADER_SIZE, BALANCE_NUMBER * 2 * 8);
            setRawBefore(raw, son);
            setRawNoKeys(raw, BALANCE_NUMBER);

            dataItem.release();
            Res res = new Res();
            res.uid = son;
            res.key = getRawKthKey(nodeRaw, BALANCE_NUMBER - 1);


            return res;
        }finally {
            dataItem.unlock();
        }


    }



    protected long createFatherNode(long uid, Type key) throws Exception {
//        SayubArr nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        byte[] nodeRaw=new byte[NODE_SIZE];
        setRawIsLeaf(nodeRaw, false);
        setRawNoKeys(nodeRaw, 2);
        setRawAfter(nodeRaw, 0);
        setRawBefore(nodeRaw, 0);
        setRawKey(nodeRaw, 0, key);
        setRawKey(nodeRaw, 1, getRawKthKey(raw, getRawNoKeys(raw) - 1));
        setRawSon(nodeRaw, 0, uid);
        setRawSon(nodeRaw, 1, this.tree.rootUid());


        long parent = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw);


        return parent;
    }

    public Type getRawKthKey(byte[] raw, int pos) {

        int offset =  NODE_HEADER_SIZE + pos * (8 * 2) + 8;

        Type type = TypeFactory.unParse8Raw(this.typeName, Arrays.copyOfRange(raw, offset, offset + 8));
        return type;
//        return type.parseToSelf(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    private void copyRawFromPos(byte[] from, byte[] to, int pos) {
        System.arraycopy(from,  NODE_HEADER_SIZE, to,  NODE_HEADER_SIZE, pos * 2 * 8);
    }

    private boolean checkNum() {
        if (getRawNoKeys(raw) == BALANCE_NUMBER * 2) {
            return true;
        }
        return false;
    }


    class Res {
        long uid;
        Type key;
    }

    private void shiftRawKeyAndSon(byte[] raw, int position, Type key, long sonUid) {
        setRawNoKeys(raw, getRawNoKeys(raw) + 1);
        int begin =  NODE_HEADER_SIZE + (position + 1) * (8 * 2);
        int end =  NODE_SIZE - 1;
        for (int i = end; i >= begin; i--) {
            raw[i] = raw[i - (8 * 2)];
        }
        setRawKey(raw, position, key);
        setRawSon(raw, position, sonUid);
    }


    private void setRawKey(byte[] raw, int position, Type key) {
        byte[] bytes = key.parseRaw();
        if (bytes.length < 8) {
            bytes = Bytes.concat(new byte[8 - bytes.length], bytes);
        }
        System.arraycopy(bytes, 0, raw,  NODE_HEADER_SIZE + position * 2 * 8 + 8, 8);
    }

    private void setRawSon(byte[] raw, int position, long sonUid) {
        System.arraycopy(Parser.unParseLong(sonUid), 0, raw,  NODE_HEADER_SIZE + position * 2 * 8, 8);
    }

    private long findLeafBound(Type key) {
        long left = 0;
        dataItem.rLock();

        try{
            long right = getRawNoKeys(raw);
            long mid;
            while (left < right) {
                mid = (left + right) / 2;
                Type midKey = getRawKthKey(raw, (int) mid);
                if (key.compareTo(midKey) == 0) {
                    right = mid;
                } else if (key.compareTo(midKey) > 0) {
                    left = mid + 1;

                } else {
                    right = mid;
                }

            }
        }finally {
            dataItem.runLock();
        }



        return left;
    }

//    private long getRawKey(SubArray raw,long position) {
//        return Parser.parseLong(Arrays.copyOfRange(raw.raw,(int)(raw.start+NODE_HEADER_SIZE+(position*2-1)*8),(int)(raw.start+NODE_HEADER_SIZE+position*2*8)));
//    }


    static byte[] newNilRootRaw() {
//        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        byte[] raw=new byte[NODE_SIZE];
        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawBefore(raw, 0);
        setRawAfter(raw, 0);

        return raw;
    }

    public static void setRawBefore(byte[] raw, long nextUid) {
        System.arraycopy(Parser.unParseLong(nextUid), 0, raw,  BEFORE_NODE_OFFSET, 8);
    }

    public static long getRawBefore(byte[] raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw,  BEFORE_NODE_OFFSET, BEFORE_NODE_OFFSET + 8));
    }

    private static void setRawAfter(byte[] raw, long nextUid) {
        System.arraycopy(Parser.unParseLong(nextUid), 0, raw,  AFTER_NODE_OFFSET, 8);
    }

    public static long getRawAfter(byte[] raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw,  AFTER_NODE_OFFSET, AFTER_NODE_OFFSET + 8));
    }

    private static void setRawNoKeys(byte[] raw, int noKeys) {
        System.arraycopy(Parser.unParseInt(noKeys), 0, raw,  KEYS_NUM_OFFSET, 4);
    }

    private static void setRawIsLeaf(byte[] raw, boolean isLeaf) {
        if (isLeaf) {
            raw[IS_LEAF_OFFSET] = (byte) 1;
        } else {
            raw[IS_LEAF_OFFSET] = (byte) 0;
        }
    }


    public static int getRawNoKeys(byte[] raw) {
        return Parser.parseInt(Arrays.copyOfRange(raw,  KEYS_NUM_OFFSET,  KEYS_NUM_OFFSET + 4));
    }

    public static boolean getRawIfLeaf(byte[] raw) {

        return raw[IS_LEAF_OFFSET] == (byte) 1;
    }


}
