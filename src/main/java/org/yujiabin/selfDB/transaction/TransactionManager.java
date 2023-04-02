package org.yujiabin.selfDB.transaction;

import org.yujiabin.selfDB.exception.UpdateXidFileException;
import org.yujiabin.selfDB.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.yujiabin.selfDB.utils.FileUtil.createNewFile;
import static org.yujiabin.selfDB.utils.FileUtil.getRandomAccessFile;

/**
 * TransactionManager 维护了一个xid文件，用来记录各个事务的状态，每个事务都有下面的三种状态：
 * 1. active，正在进行，尚未结束
 * 2. committed，已提交
 * 3. aborted，已撤销（回滚）
 * XID 文件给每个事务分配了一个字节的空间，用来保存其状态。同时，在xid文件的头部，还保存了一个 8 字节的数字，记录了这个 XID 文件管理的事务的个数。
 * 事务在文件中的状态就存储在(transaction-1)+8字节处，transaction-1是因为 transaction 0（超级事务，始终为committed）的状态不需要记录。
 */
public interface TransactionManager {
    /**
     * 开启一个事务
     * @return 事务id
     */
    long begin();

    /**
     * 提交一个事务
     * @param transaction 事务id
     */
    void commit(long transaction);

    /**
     * 取消一个事务
     * @param transaction 事务id
     */
    void abort(long transaction);

    /**
     * 判断一个事务是否正在进行
     * @param transaction 事务id
     * @return 是则返回true，反之返回false
     */
    boolean isActive(long transaction);

    /**
     * 判断一个事务是否已提交
     * @param transaction 事务id
     * @return 是则返回true，反之返回false
     */
    boolean isCommitted(long transaction);

    /**
     * 判断一个事务是否已取消
     * @param transaction 事务id
     * @return 是则返回true，反之返回false
     */
    boolean isAborted(long transaction);

    /**
     * 关闭TransactionManager
     */
    void close();

    /**
     * 在指定路径创建xid事务文件
     * @param path 文件路径
     */
    static TransactionManagerImpl createXidFile(String path){
        File file = new File(path + TransactionManagerImpl.XID_FILE_SUFFIX);
        createNewFile(file);
        RandomAccessFile raf = getRandomAccessFile(file);
        FileChannel fc = raf.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(TransactionManagerImpl.XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.write(buffer);
        } catch (IOException e) {
            Panic.panic(new UpdateXidFileException("update xidFile's head error"));
        }
        return new TransactionManagerImpl(raf, fc);
    }

    /**
     * 打开指定路径的xid事务文件
     * @param path 文件路径
     */
    static TransactionManagerImpl openXidFile(String path){
        File file = new File(path + TransactionManagerImpl.XID_FILE_SUFFIX);
        if (!file.exists())
            Panic.panic(new FileNotFoundException("the file not found"));
        RandomAccessFile raf = getRandomAccessFile(file);
        FileChannel fc = raf.getChannel();
        return new TransactionManagerImpl(raf, fc);
    }
}