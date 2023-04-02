package org.yujiabin.selfDB.data;

import org.yujiabin.selfDB.data.dataItem.DataItem;
import org.yujiabin.selfDB.data.logger.Logger;
import org.yujiabin.selfDB.data.page.PageCache;
import org.yujiabin.selfDB.data.page.PageFirst;
import org.yujiabin.selfDB.data.recover.Recover;
import org.yujiabin.selfDB.transaction.TransactionManager;

public interface DataManager {
    /**
     * 通过uid获取数据项
     */
    DataItem getDataItem(long uid) throws Exception;

    /**
     * 新增数据项
     * @param transaction 事务id
     * @param data 数据
     * @return uid
     */
    long insertDataItem(long transaction, byte[] data) throws Exception;

    /**
     * 关闭DataManager
     */
    void close();

    /**
     * 创建db数据文件和log日志文件
     * @param path 文件路径
     * @param mem 内存大小
     * @param tm 事务管理对象
     */
    static DataManager createFile(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.createDbFile(path, mem);
        Logger lg = Logger.createLogFile(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageFirst();
        return dm;
    }

    /**
     * 打开db数据文件和log日志文件
     * @param path 文件路径
     * @param mem 内存大小
     * @param tm 事务管理对象
     */
    static DataManager openFile(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.openDbFile(path, mem);
        Logger lg = Logger.openLogFile(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if(!dm.loadCheckPageFirst()) {
            Recover.recover(tm, lg, pc);
        }
        dm.initPageIndex();
        PageFirst.setVcOpen(dm.pageFirst);
        dm.pc.flushPage(dm.pageFirst);
        return dm;
    }
}
