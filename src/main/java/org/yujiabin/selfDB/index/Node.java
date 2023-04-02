package org.yujiabin.selfDB.index;

import org.yujiabin.selfDB.data.dataItem.DataItem;
import org.yujiabin.selfDB.transaction.TransactionManagerImpl;
import org.yujiabin.selfDB.utils.Parser;
import org.yujiabin.selfDB.utils.vo.SubArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid][Son0][Key0][Son1][Key1]...[SonN][KeyN]
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    /**
     * 设置该节点为叶子节点
     * @param raw 节点
     * @param isLeaf true 或 false
     */
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.arr[raw.start + IS_LEAF_OFFSET] = (byte)1;
        } else {
            raw.arr[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }

    /**
     * 判断该节点是否是叶子节点
     * @param raw 节点
     */
    static boolean rawIsLeaf(SubArray raw) {
        return raw.arr[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    /**
     * 设置节点有key的数量
     * @param raw 节点
     * @param keysNumber key的数量
     */
    static void setRawKeysNumber(SubArray raw, int keysNumber) {
        System.arraycopy(Parser.shortToByteArray((short) keysNumber), 0, raw.arr, raw.start+NO_KEYS_OFFSET, 2);
    }

    /**
     * 获取节点中key的数量
     * @param raw 节点
     */
    static int getRawKeysNumber(SubArray raw) {
        return Parser.byteArraysToShort(Arrays.copyOfRange(raw.arr, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }

    /**
     * 设置节点的兄弟节点
     * @param raw 节点
     * @param sibling 兄弟节点uid
     */
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.longToByteArray(sibling), 0, raw.arr, raw.start+SIBLING_OFFSET, 8);
    }

    /**
     * 获取节点的兄弟节点uid
     * @param raw 节点
     */
    static long getRawSibling(SubArray raw) {
        return Parser.byteArrayToLong(Arrays.copyOfRange(raw.arr, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }

    /**
     * 设置节点第kth个子节点uid
     * @param raw 节点
     * @param uid 子节点uid
     * @param kth 字节点位置
     */
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.longToByteArray(uid), 0, raw.arr, offset, 8);
    }

    /**
     * 获取节点第kth个子节点uid
     * @param raw 节点
     * @param kth 子节点位置
     * @return 子节点uid
     */
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.byteArrayToLong(Arrays.copyOfRange(raw.arr, offset, offset+8));
    }

    /**
     * 设置节点第kth个子节点key
     * @param raw 节点
     * @param key 子节点key
     * @param kth 子节点位置
     */
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.longToByteArray(key), 0, raw.arr, offset, 8);
    }

    /**
     * 获取节点第kth个子节点key
     * @param raw 节点
     * @param kth 子节点位置
     * @return 子节点key
     */
    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.byteArrayToLong(Arrays.copyOfRange(raw.arr, offset, offset+8));
    }

    /**
     * 复制节点from第kth个子节点及之后的到节点to中
     * @param from 被复制的节点
     * @param to 被粘贴的节点
     * @param kth 位置
     */
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.arr, offset, to.arr, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    /**
     * 将第kth个子节点后的所有子节点左移一个位置
     * @param raw 节点
     * @param kth 位置
     */
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.arr[i] = raw.arr[i-(8*2)];
        }
    }

    /**
     * 创建根节点
     * @param left 左子节点
     * @param right 右子节点
     * @param key 左子节点key
     * @return 根节点
     */
    static byte[] newRootRaw(long left, long right, long key)  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, false);
        setRawKeysNumber(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);
        return raw.arr;
    }

    /**
     * 创建空的根节点
     */
    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, true);
        setRawKeysNumber(raw, 0);
        setRawSibling(raw, 0);
        return raw.arr;
    }

    /**
     * 装填一个Node节点
     * @param bTree b+树
     * @param uid 节点uid
     */
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.getDataItem(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.getData();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    /**
     * 判断节点是否为叶子节点
     */
    public boolean isLeaf() {
        dataItem.readLock();
        try {
            return rawIsLeaf(raw);
        } finally {
            dataItem.unReadLock();
        }
    }

    /**
     * 查找子节点uid结果集
     */
    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    /**
     * 在该节点寻找对应key的子节点uid
     * @return 找到则返回子节点uid，未找到则返回兄弟节点uid
     */
    public SearchNextRes searchNext(long key) {
        dataItem.readLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int keysNumber = getRawKeysNumber(raw);
            for(int i = 0; i < keysNumber; i ++) {
                long ik = getRawKthKey(raw, i);
                if(key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.unReadLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    /**
     * 在当前节点进行范围查找[leftKey,rightKey]
     * @param leftKey 左边界
     * @param rightKey 右边界
     * @return 若rightKey大于该节点最大key，则同时返回兄弟节点uid
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.readLock();
        try {
            int noKeys = getRawKeysNumber(raw);
            int kth = 0;
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey) {
                    break;
                }
                kth ++;
            }
            List<Long> uids = new ArrayList<>();
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }
            long siblingUid = 0;
            if(kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.unReadLock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    /**
     * 插入数据，如果超过节点树的阶层，则分割节点
     * @return 如果插入失败返回兄弟节点uid；如果插入成功且不需要分割节点则返回null，若需要分割则返回子节点uid和key
     */
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.beforeModify();
        try {
            success = insert(uid, key);
            if(!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            if(needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if(err == null && success) {
                dataItem.afterModify(TransactionManagerImpl.SUPER_TRANSACTION);
            } else {
                dataItem.revocationModify();
            }
        }
    }

    /**
     * 插入数据
     */
    private boolean insert(long uid, long key) {
        int noKeys = getRawKeysNumber(raw);
        int kth = 0;
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        if(kth == noKeys && getRawSibling(raw) != 0) return false;
        if(rawIsLeaf(raw)) {
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawKeysNumber(raw, noKeys+1);
        } else {
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth+1);
            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);
            setRawKeysNumber(raw, noKeys+1);
        }
        return true;
    }

    /**
     * 判断节点是否需要分割
     */
    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawKeysNumber(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    /**
     * 分割节点
     */
    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, rawIsLeaf(raw));
        setRawKeysNumber(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        long son = tree.dm.insertDataItem(TransactionManagerImpl.SUPER_TRANSACTION, nodeRaw.arr);
        setRawKeysNumber(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);
        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(rawIsLeaf(raw)).append("\n");
        int KeyNumber = getRawKeysNumber(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}
