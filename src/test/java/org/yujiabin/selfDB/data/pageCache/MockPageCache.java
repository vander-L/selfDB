package org.yujiabin.selfDB.data.pageCache;

import org.yujiabin.selfDB.data.page.MockPage;
import org.yujiabin.selfDB.data.page.Page;
import org.yujiabin.selfDB.data.page.PageCache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MockPageCache implements PageCache {

    private Map<Integer, MockPage> cache = new HashMap<>();
    private Lock lock = new ReentrantLock();
    private AtomicInteger noPages = new AtomicInteger(0);
    
    @Override
    public int createNewPage(byte[] initData) {
        lock.lock();
        try {
            int pgno = noPages.incrementAndGet();
            MockPage pg = MockPage.newMockPage(pgno, initData);
            cache.put(pgno, pg);
            return pgno;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        lock.lock();
        try {
            return cache.get(pgno);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {}

    @Override
    public void releasePage(Page page) {}

    @Override
    public void truncateByPageNumber(int maxPgno) {}

    @Override
    public int getPageNumbers() {
        return noPages.intValue();
    }

    @Override
    public void flushPage(Page pg) {}
    
}
