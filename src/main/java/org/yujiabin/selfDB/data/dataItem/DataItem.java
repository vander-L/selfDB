package org.yujiabin.selfDB.data.dataItem;

import com.google.common.primitives.Bytes;
import org.yujiabin.selfDB.utils.vo.SubArray;
import org.yujiabin.selfDB.data.DataManagerImpl;
import org.yujiabin.selfDB.data.page.Page;
import org.yujiabin.selfDB.utils.Parser;

import java.util.Arrays;

public interface DataItem {
    /**
     * 获取DataItem中的数据
     * @return SubArray对象
     */
    SubArray getData();

    /**
     * 修改数据前所需进行的操作
     */
    void beforeModify();

    /**
     * 撤销修改
     */
    void revocationModify();

    /**
     * 修改数据后所需进行的操作
     * @param transaction 事务id
     */
    void afterModify(long transaction);

    /**
     * 释放DataItem
     */
    void release();

    /**
     * 对DataItem的操作加写锁
     */
    void writeLock();

    /**
     * 释放对DataItem所加的写锁
     */
    void unWriteLock();

    /**
     * 对DataItem的操作加读锁
     */
    void readLock();

    /**
     * 释放对DataItem所加的读锁
     */
    void unReadLock();

    /**
     * 获取数据所处的页
     */
    Page getPage();

    /**
     * 获取数据的uid
     */
    long getUid();

    /**
     * 获取旧数据项
     */
    byte[] getOldRaw();

    /**
     * 获取数据项
     */
    SubArray getRaw();

    /**
     * 包装数据为数据项
     * @param raw 数据
     */
    static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.shortToByteArray((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }

    /**
     * 将页中数据转化为数据项
     * @param pg 页
     * @param offset 数据在页中的偏移量
     * @param dm DataManagerImpl对象
     */
    static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.byteArraysToShort(Arrays.copyOfRange(raw, offset+DataItemImpl.OF_SIZE, offset+DataItemImpl.OF_DATA));
        short length = (short)(size + DataItemImpl.OF_DATA);
        long uid = Parser.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], pg, uid, dm);
    }

    /**
     * 设置数据项为无效
     */
    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_FLAG] = (byte)1;
    }
}
