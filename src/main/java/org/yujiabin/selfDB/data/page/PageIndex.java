package org.yujiabin.selfDB.data.page;

import org.yujiabin.selfDB.data.page.vo.PageInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    private static final int BLOCK_NUMBER = 40;
    private static final int BLOCK_MEMORY = PageCache.PAGE_SIZE / BLOCK_NUMBER;
    private Lock lock;
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[BLOCK_NUMBER+1];
        for (int i = 0; i < BLOCK_NUMBER+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 添加页面
     * @param pageNumber 页号
     * @param freeSpace 可分配空间
     */
    public void add(int pageNumber, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / BLOCK_MEMORY;
            lists[number].add(new PageInfo(pageNumber, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取页面
     * @param spaceSize 所需空间大小
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / BLOCK_MEMORY;
            if(number < BLOCK_NUMBER) number ++;
            while(number <= BLOCK_NUMBER) {
                if(lists[number].size() == 0) {
                    number ++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
