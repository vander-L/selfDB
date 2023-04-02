package org.yujiabin.selfDB.version;

import org.yujiabin.selfDB.transaction.TransactionManager;

public class Visibility {

    /**
     * 判断是否发生版本跳跃
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long deleteTran = e.getTranDelete();
        if(t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(deleteTran) && (deleteTran > t.xid || t.isInSnapshot(deleteTran));
        }
    }

    /**
     * 判断记录e对事务t是否可见
     * @param tm
     * @param t 事务
     * @param e 记录
     */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 读提交隔离级别，判断记录e对事务t是否可见
     * @param tm
     * @param t 事务
     * @param e 记录
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long updateTran = e.getTranUpdate();
        long deleteTran = e.getTranDelete();
        if(updateTran == xid && deleteTran == 0) return true;

        if(tm.isCommitted(updateTran)) {
            if(deleteTran == 0) return true;
            if(deleteTran != xid) {
                return !tm.isCommitted(deleteTran);
            }
        }
        return false;
    }

    /**
     * 可重复读隔离级别，判断记录e对事务t是否可见
     * @param tm
     * @param t 事务
     * @param e 记录
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long updateTran = e.getTranUpdate();
        long deleteTran = e.getTranDelete();
        if(updateTran == xid && deleteTran == 0) return true;

        if(tm.isCommitted(updateTran) && updateTran < xid && !t.isInSnapshot(updateTran)) {
            if(deleteTran == 0) return true;
            if(deleteTran != xid) {
                if(!tm.isCommitted(deleteTran) || deleteTran > xid || t.isInSnapshot(deleteTran)) {
                    return true;
                }
            }
        }
        return false;
    }

}
