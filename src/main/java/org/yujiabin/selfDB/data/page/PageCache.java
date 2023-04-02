package org.yujiabin.selfDB.data.page;

import org.yujiabin.selfDB.exception.CreateFileException;
import org.yujiabin.selfDB.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static org.yujiabin.selfDB.utils.FileUtil.createNewFile;
import static org.yujiabin.selfDB.utils.FileUtil.getRandomAccessFile;

public interface PageCache {

    int PAGE_SIZE = 1 << 13;

    /**
     * 创建一个新页
     * @param initData 初始的数据
     * @return 该页的页号
     */
    int createNewPage(byte[] initData);

    /**
     * 通过页号来获取页面
     * @param pageNumber 页号
     * @return Page对象，即页面
     */
    Page getPage(int pageNumber) throws Exception;

    /**
     * 关闭PageCache
     */
    void close();

    /**
     * 将该页从缓存中释放
     * @param page 页面（Page对象）
     */
    void releasePage(Page page);

    /**
     * 通过页号来截断页面
     * @param maxPageNumber 页号
     */
    void truncateByPageNumber(int maxPageNumber);

    /**
     * 获取缓存中页的数量
     * @return 页的数量
     */
    int getPageNumbers();

    /**
     * 将页面刷进硬盘
     */
    void flushPage(Page page);

    /**
     * 在指定位置创建db数据文件
     * @param path 文件路径
     * @param memory 内存大小
     */
    static PageCacheImpl createDbFile(String path, long memory) {
        File file = new File(path+PageCacheImpl.DB_SUFFIX);
        createNewFile(file);
        RandomAccessFile raf = getRandomAccessFile(file);
        FileChannel fc = raf.getChannel();
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    /**
     * 打开指定路径的db数据文件
     * @param path 文件路径
     * @param memory 内存大小
     */
    static PageCacheImpl openDbFile(String path, long memory) {
        File file = new File(path+PageCacheImpl.DB_SUFFIX);
        if (!file.exists())
            Panic.panic(new FileNotFoundException("the file not found"));
        RandomAccessFile raf = getRandomAccessFile(file);
        FileChannel fc = raf.getChannel();
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}
