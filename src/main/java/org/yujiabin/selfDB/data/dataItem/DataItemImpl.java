package org.yujiabin.selfDB.data.dataItem;

import org.yujiabin.selfDB.data.DataManagerImpl;
import org.yujiabin.selfDB.data.page.Page;
import org.yujiabin.selfDB.utils.vo.SubArray;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * DataItem:[Flag][DataSize][Data]
 * Flag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem {

    public static final int OF_FLAG = 0;
    public static final int OF_SIZE = 1;
    public static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock readLock;
    private Lock writeLock;
    private DataManagerImpl dm;
    private long uid;
    private Page pg;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    /**
     * 判断数据项是否合法
     */
    public boolean isValid() {
        return raw.arr[raw.start+ OF_FLAG] == (byte)0;
    }

    @Override
    public SubArray getData() {
        return new SubArray(raw.arr, raw.start+OF_DATA, raw.end);
    }

    @Override
    public void beforeModify() {
        writeLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.arr, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void revocationModify() {
        System.arraycopy(oldRaw, 0, raw.arr, raw.start, oldRaw.length);
        writeLock.unlock();
    }

    @Override
    public void afterModify(long transaction) {
        dm.logDataItem(transaction, this);
        writeLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void writeLock() {
        writeLock.lock();
    }

    @Override
    public void unWriteLock() {
        writeLock.unlock();
    }

    @Override
    public void readLock() {
        readLock.lock();
    }

    @Override
    public void unReadLock() {
        readLock.unlock();
    }

    @Override
    public Page getPage() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
    
}
