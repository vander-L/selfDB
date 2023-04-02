package org.yujiabin.selfDB.data.recover;

import com.google.common.primitives.Bytes;
import org.yujiabin.selfDB.utils.vo.SubArray;
import org.yujiabin.selfDB.data.dataItem.DataItem;
import org.yujiabin.selfDB.data.logger.Logger;
import org.yujiabin.selfDB.data.page.Page;
import org.yujiabin.selfDB.data.page.PageCache;
import org.yujiabin.selfDB.data.page.PageCommon;
import org.yujiabin.selfDB.transaction.TransactionManager;
import org.yujiabin.selfDB.data.logger.vo.InsertLogInfo;
import org.yujiabin.selfDB.utils.Panic;
import org.yujiabin.selfDB.utils.Parser;
import org.yujiabin.selfDB.data.logger.vo.UpdateLogInfo;

import java.util.*;

/**
 * updateLog:[LogType][Transaction][UID][OldRaw][NewRaw]
 * insertLog:[LogType][Transaction][PageNumber][Offset][Raw]
 */
public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;
    private static final int REDO = 0;
    private static final int UNDO = 1;
    private static final int OF_TYPE = 0;
    private static final int OF_TRANSACTION = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_TRANSACTION + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;
    private static final int OF_INSERT_PAGE_NUMBER = OF_TRANSACTION + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PAGE_NUMBER + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    /**
     * 依据日志文件进行恢复策略
     * @param tm 事务
     * @param lg 日志
     * @param pc 页缓存
     */
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();
        int maxPageNumber = 0;
        while(true) {
            byte[] log = lg.nextLog();
            if(log == null) break;
            int pgno;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pageNumber;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pageNumber;
            }
            if(pgno > maxPageNumber) {
                maxPageNumber = pgno;
            }
        }
        if(maxPageNumber == 0) {
            maxPageNumber = 1;
        }
        pc.truncateByPageNumber(maxPageNumber);
        System.out.println("Truncate to " + maxPageNumber + " pages.");

        //这一步,通过log生成重做日志,然后执行重做日志
        redoTransactions(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        //这一步,通过log生成回滚日志,然后执行回滚日志
        undoTransactions(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    /**
     * insertLog:[LogType][Transaction][PageNumber][Offset][Raw]
     * 根据事务，页，数据来生成Insert日志
     * @param transaction 事务
     * @param page 页
     * @param raw 数据
     * @return Insert日志
     */
    public static byte[] getInsertLog(long transaction, Page page, byte[] raw){
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.longToByteArray(transaction);
        byte[] pgnoRaw = Parser.intToByteArray(page.getPageNumber());
        byte[] offsetRaw = Parser.shortToByteArray(PageCommon.getFSO(page));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    /**
     * updateLog:[LogType][Transaction][UID][OldRaw][NewRaw]
     * 根据事务、数据项（DataItem）生成Update日志
     * @param transaction 事务
     * @param di 数据项
     * @return Update日志
     */
    public static byte[] getUpdateLog(long transaction, DataItem di){
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.longToByteArray(transaction);
        byte[] uidRaw = Parser.longToByteArray(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.arr, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    /**
     * 重做已完成的事务
     * @param tm 事务
     * @param lg 日志
     * @param pc 页缓存
     */
    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc){
        lg.rewind();
        while(true) {
            byte[] log = lg.nextLog();
            if(log == null) break;
            //遍历判断每条日志是 插入还是更新
            if(isInsertLog(log)) {
                //插入操作

                InsertLogInfo li = parseInsertLog(log);
                //取出log的xid,事务id
                long xid = li.transaction;

                //通过事务id判断该事务是否是active
                //在重做日志中,非active状态的事物才需要重做
                if(!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                //更新操作

                UpdateLogInfo xi = parseUpdateLog(log);
                //取出log的xid,事务id
                long xid = xi.transaction;
                //通过事务id判断该事务是否是active
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    /**
     * 回滚未完成的事务
     * @param tm 事务
     * @param lg 日志
     * @param pc 页缓存
     */
    private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc){
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while(true) {
            byte[] log = lg.nextLog();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                //取出log的xid,事务id
                long xid = li.transaction;
                //通过事务id判断该事务是否是active
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                //取出log的xid,事务id
                long xid = xi.transaction;

                //通过事务id判断该事务是否是active
                //在回滚日志中,active状态的事物才需要回滚
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo
        for(Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    /**
     * 判断日志是否为Insert日志
     */
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    /**
     * 判断日志是否为Update日志
     */
    private static boolean isUpdateLog(byte[] log) {
        return log[0] == LOG_TYPE_UPDATE;
    }

    /**
     * 将日志转换为InsertLogInfo对象
     */
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        //取出log中的xid,事务id
        li.transaction = Parser.byteArrayToLong(Arrays.copyOfRange(log, OF_TRANSACTION, OF_INSERT_PAGE_NUMBER));
        li.pageNumber = Parser.byteArraysToInt(Arrays.copyOfRange(log, OF_INSERT_PAGE_NUMBER, OF_INSERT_OFFSET));
        li.offset = Parser.byteArraysToShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.data = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    /**
     * 将日志转换为UpdateLogInfo对象
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        //取出log中的xid,事务id
        li.transaction = Parser.byteArrayToLong(Arrays.copyOfRange(log, OF_TRANSACTION, OF_UPDATE_UID));
        long uid = Parser.byteArrayToLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pageNumber = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldData = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newData = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    /**
     * 根据相应的参数来执行Insert日志
     * @param pc 页缓存
     * @param log Insert日志
     * @param flag 执行的防止（redo，undo）
     */
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pageNumber);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            if(flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.data);
            }
            PageCommon.recoverInsert(pg, li.data, li.offset);
        } finally {
            pg.release();
        }
    }

    /**
     * 根据相应的参数来执行Update日志
     * @param pc 页缓存
     * @param log Update日志
     * @param flag 执行的防止（redo，undo）
     */
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pageNumber;
        short offset;
        byte[] raw;
        if(flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pageNumber = xi.pageNumber;
            offset = xi.offset;
            raw = xi.newData;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pageNumber = xi.pageNumber;
            offset = xi.offset;
            raw = xi.oldData;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pageNumber);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageCommon.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }
}
