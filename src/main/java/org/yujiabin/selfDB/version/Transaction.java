package org.yujiabin.selfDB.version;

import org.yujiabin.selfDB.transaction.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * 抽象事务，用于保存快照
 */
public class Transaction {
    public long xid;
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    public static Transaction getTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    /**
     * 判断事务是否是快照版本
     */
    public boolean isInSnapshot(long transaction) {
        if(transaction == TransactionManagerImpl.SUPER_TRANSACTION) {
            return false;
        }
        return snapshot.containsKey(transaction);
    }
}