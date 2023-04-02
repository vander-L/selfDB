package org.yujiabin.selfDB.data.dataItem;

import org.yujiabin.selfDB.data.page.Page;
import org.yujiabin.selfDB.utils.vo.SubArray;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MockDataItem implements DataItem {

    private SubArray data;
    private byte[] oldData;
    private long uid;
    private Lock rLock;
    private Lock wLock;

    public static MockDataItem newMockDataItem(long uid, SubArray data) {
        MockDataItem di = new MockDataItem();
        di.data = data;
        di.oldData = new byte[data.end - data.start];
        di.uid = uid;
        ReadWriteLock l = new ReentrantReadWriteLock();
        di.rLock = l.readLock();
        di.wLock = l.writeLock();
        return di;
    }

    @Override
    public SubArray getData() {
        return data;
    }

    @Override
    public void beforeModify() {
        wLock.lock();
        System.arraycopy(data.arr, data.start, oldData, 0, oldData.length);
    }

    @Override
    public void revocationModify() {
        System.arraycopy(oldData, 0, data.arr, data.start, oldData.length);
        wLock.unlock();
    }

    @Override
    public void afterModify(long transaction) {
        wLock.unlock();
    }

    @Override
    public void release() {}

    @Override
    public void writeLock() {
        wLock.lock();
    }

    @Override
    public void unWriteLock() {
        wLock.unlock();
    }

    @Override
    public void readLock() {
        rLock.lock();
    }

    @Override
    public void unReadLock() {
        rLock.unlock();
    }

    @Override
    public Page getPage() {
        return null;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldData;
    }

    @Override
    public SubArray getRaw() {
        return data;
    }
    
}
