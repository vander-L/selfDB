package org.yujiabin.selfDB.version;

import org.yujiabin.selfDB.data.DataManager;
import org.yujiabin.selfDB.transaction.TransactionManager;

public interface VersionManager {
    /**
     * 读取资源
     * @param tran 事务
     * @param uid 资源id
     */
    byte[] read(long tran, long uid) throws Exception;

    /**
     * 写入资源
     * @param tran 事务
     * @param data 数据
     */
    long insert(long tran, byte[] data) throws Exception;

    /**
     * 删除资源
     * @param tran 事务
     * @param uid 资源id
     */
    boolean delete(long tran, long uid) throws Exception;

    /**
     * 开启一个隔离级别为level的事务
     * @param level 隔离级别
     * @return 事务id
     */
    long begin(int level);

    /**
     * 提交一个事务
     * @param tran 事务id
     */
    void commit(long tran) throws Exception;

    /**
     * 手动中止一个事务
     * @param tran 事务id
     */
    void abort(long tran);

    static VersionManager getVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}
