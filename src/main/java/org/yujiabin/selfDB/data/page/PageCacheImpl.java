package org.yujiabin.selfDB.data.page;

import org.yujiabin.selfDB.common.AbstractCache;
import org.yujiabin.selfDB.exception.MemTooSmallException;
import org.yujiabin.selfDB.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";
    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock lock;
    private AtomicInteger pageNumbers;      //缓存中页的数量
    PageCacheImpl(RandomAccessFile raf, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(new MemTooSmallException());
        }
        long length = 0;
        try {
            length = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.raf = raf;
        this.fc = fileChannel;
        this.lock = new ReentrantLock();
        //构造方法初始化时,先根据file的大小,除PageSize 得出有多少页
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    @Override
    protected Page getDataNotInCache(long key){
        int pageNumber = (int)key;
        long offset = pageOffset(pageNumber);
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        lock.lock();
        try{
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
        return new PageImpl(pageNumber, buf.array(), this);
    }

    @Override
    protected void releaseFromCache(Page page) {
        if (page.isDirty()){
            flush(page);
            page.setDirty(false);
        }
    }

    @Override
    public int createNewPage(byte[] initData) {
        int pageNumber = pageNumbers.incrementAndGet();
        flush(new PageImpl(pageNumber, initData, null));
        return pageNumber;
    }

    @Override
    public Page getPage(int pageNumber) throws Exception {
        return getResources(pageNumber);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void releasePage(Page page) {
        releaseResource(page.getPageNumber());
    }

    @Override
    public void truncateByPageNumber(int maxPageNumber) {
        long size = pageOffset(maxPageNumber + 1);
        try {
            raf.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPageNumber);
    }

    @Override
    public int getPageNumbers() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }

    /**
     * 获取页面在文件中的偏移量
     * @param pageNumber 页号
     * @return 偏移量
     */
    private static long pageOffset(int pageNumber){
        return (long) (pageNumber - 1) * PAGE_SIZE;
    }

    /**
     * 将页面刷到磁盘中
     */
    private void flush(Page page){
        int pageNumber = page.getPageNumber();
        long offset = pageOffset(pageNumber);
        lock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
    }
}
