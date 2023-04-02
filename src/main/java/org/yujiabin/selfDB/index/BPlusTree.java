package org.yujiabin.selfDB.index;

import org.yujiabin.selfDB.data.DataManager;
import org.yujiabin.selfDB.data.dataItem.DataItem;
import org.yujiabin.selfDB.transaction.TransactionManagerImpl;
import org.yujiabin.selfDB.utils.Parser;
import org.yujiabin.selfDB.utils.vo.SubArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BPlusTree {
    DataManager dm;
    long rootUid;
    DataItem rootDataItem;
    Lock bootLock;

    /**
     * 创建b+树的一个空根节点
     * @return 该节点所存储在数据项的uid
     */
    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insertDataItem(TransactionManagerImpl.SUPER_TRANSACTION, rawRoot);
        return dm.insertDataItem(TransactionManagerImpl.SUPER_TRANSACTION, Parser.longToByteArray(rootUid));
    }

    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.getDataItem(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.rootUid = bootUid;
        t.dm = dm;
        t.rootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    /**
     * 获取根节点的uid
     */
    private long getRootUid() {
        bootLock.lock();
        try {
            SubArray sa = rootDataItem.getData();
            return Parser.byteArrayToLong(Arrays.copyOfRange(sa.arr, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 修改根节点
     * @param left 左子节点
     * @param right 右子节点
     * @param rightKey 右子节点key
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insertDataItem(TransactionManagerImpl.SUPER_TRANSACTION, rootRaw);
            rootDataItem.beforeModify();
            SubArray diRaw = rootDataItem.getData();
            System.arraycopy(Parser.longToByteArray(newRootUid), 0, diRaw.arr, diRaw.start, 8);
            rootDataItem.afterModify(TransactionManagerImpl.SUPER_TRANSACTION);
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 查找叶子节点
     * @param nodeUid 节点uid
     * @param key 节点key
     * @return 叶子节点uid
     */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();
        if(isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    /**
     * 查找子节点，若无子节点则返回兄弟节点
     * @param nodeUid 节点uid
     * @param key 节点key
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    /**
     * 查找节点key为key的节点
     */
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    /**
     * 查找节点key在[leftKey,rightKey]间的的节点
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = getRootUid();
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    /**
     * 插入数据
     */
    public void insert(long key, long uid) throws Exception {
        long rootUid = getRootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    static class InsertRes {
        long newNode, newKey;
    }

    /**
     * 插入数据，并根据情况分割节点，通过调用私有方法实现
     * @param nodeUid 节点uid
     * @param uid 数据uid
     * @param key 数据key
     * @return 如果不需要分割节点则返回null，若需要分割则返回子节点uid和key
     */
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();
        InsertRes res;
        if(isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if(ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    /**
     * 插入数据，并根据情况分割节点
     * @param nodeUid 节点uid
     * @param uid 数据uid
     * @param key 数据key
     * @return 如果不需要分割节点则返回null，若需要分割则返回子节点uid和key
     */
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        rootDataItem.release();
    }
}
