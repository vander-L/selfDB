package org.yujiabin.selfDB.version;

import org.yujiabin.selfDB.common.AbstractCache;
import org.yujiabin.selfDB.data.DataManager;
import org.yujiabin.selfDB.exception.ConcurrentUpdateException;
import org.yujiabin.selfDB.exception.NullEntryException;
import org.yujiabin.selfDB.transaction.TransactionManager;
import org.yujiabin.selfDB.transaction.TransactionManagerImpl;
import org.yujiabin.selfDB.utils.Panic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_TRANSACTION, Transaction.getTransaction(TransactionManagerImpl.SUPER_TRANSACTION, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    @Override
    public byte[] read(long tran, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(tran);
        lock.unlock();
        if(t.err != null) {
            throw t.err;
        }
        Entry entry;
        try {
            entry = super.getResources(uid);
        } catch(Exception e) {
            if(e instanceof NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.getData();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long tran, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(tran);
        lock.unlock();
        if(t.err != null) {
            throw t.err;
        }
        byte[] raw = Entry.wrapEntryRaw(tran, data);
        return dm.insertDataItem(tran, raw);
    }

    @Override
    public boolean delete(long tran, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(tran);
        lock.unlock();
        if(t.err != null) {
            throw t.err;
        }
        Entry entry;
        try {
            entry = super.getResources(uid);
        } catch(Exception e) {
            if(e instanceof NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l;
            try {
                l = lt.add(tran, uid);
            } catch(Exception e) {
                t.err = new ConcurrentUpdateException();
                internAbort(tran, true);
                t.autoAborted = true;
                throw t.err;
            }
            if(l != null) {
                l.lock();
                l.unlock();
            }

            if(entry.getTranDelete() == tran) {
                return false;
            }

            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = new ConcurrentUpdateException();
                internAbort(tran, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setTranDelete(tran);
            return true;

        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long tran = tm.begin();
            Transaction t = Transaction.getTransaction(tran, level, activeTransaction);
            activeTransaction.put(tran, t);
            return tran;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long tran) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(tran);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(tran);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }
        lock.lock();
        activeTransaction.remove(tran);
        lock.unlock();
        lt.remove(tran);
        tm.commit(tran);
    }

    @Override
    public void abort(long tran) {
        internAbort(tran, false);
    }

    /**
     * 自动中止一个事务
     */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();
        if(t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }

    /**
     * 释放一个记录
     */
    public void releaseEntry(Entry entry) {
        super.releaseResource(entry.getUid());
    }

    @Override
    protected Entry getDataNotInCache(long uid) throws Exception {
        Entry entry = Entry.getEntry(this, uid);
        if(entry == null) {
            throw new NullEntryException();
        }
        return entry;
    }

    @Override
    protected void releaseFromCache(Entry entry) {
        entry.remove();
    }

}
