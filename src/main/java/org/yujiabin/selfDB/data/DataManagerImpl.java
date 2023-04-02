package org.yujiabin.selfDB.data;

import org.yujiabin.selfDB.common.AbstractCache;
import org.yujiabin.selfDB.data.dataItem.DataItem;
import org.yujiabin.selfDB.data.dataItem.DataItemImpl;
import org.yujiabin.selfDB.data.logger.Logger;
import org.yujiabin.selfDB.data.page.*;
import org.yujiabin.selfDB.data.recover.Recover;
import org.yujiabin.selfDB.exception.DataTooLargeException;
import org.yujiabin.selfDB.exception.DatabaseBusyException;
import org.yujiabin.selfDB.transaction.TransactionManager;
import org.yujiabin.selfDB.data.page.vo.PageInfo;
import org.yujiabin.selfDB.utils.Panic;
import org.yujiabin.selfDB.utils.Parser;

import java.util.Arrays;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pageIndex;
    Page pageFirst;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pageIndex = new PageIndex();
    }

    @Override
    public DataItem getDataItem(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.getResources(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insertDataItem(long transaction, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageCommon.MAX_FREE_SPACE) {
            throw new DataTooLargeException();
        }

        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pageIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPageNumber = pc.createNewPage(PageCommon.initRaw());
                pageIndex.add(newPageNumber, PageCommon.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw new DatabaseBusyException();
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pageNumber);
            byte[] log = Recover.getInsertLog(transaction, pg, raw);
            logger.log(log);

            short offset = PageCommon.insert(pg, raw);

            pg.release();
            return Parser.addressToUid(pi.pageNumber, offset);

        } finally {
            if(pg != null) {
                pageIndex.add(pi.pageNumber, PageCommon.getFreeSpace(pg));
            } else {
                pageIndex.add(pi.pageNumber, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageFirst.setVcClose(pageFirst);
        pageFirst.release();
        pc.close();
    }

    /**
     * 为事务生成update日志
     * @param transaction 事务
     * @param di 数据项
     */
    public void logDataItem(long transaction, DataItem di) {
        byte[] log = Recover.getUpdateLog(transaction, di);
        logger.log(log);
    }

    /**
     * 释放数据项
     */
    public void releaseDataItem(DataItem di) {
        super.releaseResource(di.getUid());
    }

    @Override
    protected DataItem getDataNotInCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pageNumber = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pageNumber);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseFromCache(DataItem di) {
        di.getPage().release();
    }

    /**
     * 在创建文件时初始化PageFirst
     */
    void initPageFirst() {
        int pageNumber = pc.createNewPage(PageFirst.InitRaw());
        assert pageNumber == 1;
        try {
            pageFirst = pc.getPage(pageNumber);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageFirst);
    }

    /**
     * 在打开已有文件时读入PageFirst，并验证正确性
     */
    boolean loadCheckPageFirst() {
        try {
            pageFirst = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageFirst.checkVc(pageFirst);
    }

    /**
     * 初始化pageIndex
     */
    void initPageIndex() {
        int pageNumber = pc.getPageNumbers();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pageIndex.add(pg.getPageNumber(), PageCommon.getFreeSpace(pg));
            pg.release();
        }
    }
    
}
