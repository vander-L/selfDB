package org.yujiabin.selfDB.transaction;

import org.yujiabin.selfDB.exception.CloseFileException;
import org.yujiabin.selfDB.exception.GetFromXidFileException;
import org.yujiabin.selfDB.exception.UpdateXidFileException;
import org.yujiabin.selfDB.exception.XidFileBadException;
import org.yujiabin.selfDB.transaction.TransactionManager;
import org.yujiabin.selfDB.utils.Panic;
import org.yujiabin.selfDB.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager {

    public static final int XID_HEADER_LENGTH = 8;     // XID文件头长度
    private static final int TRANSACTION_SIZE = 1;    // 每个事务的占用长度
    private static final byte TRAN_STATUS_ACTIVE = 0;   // 事务的三种状态-运行
    private static final byte TRAN_STATUS_COMMITTED = 1;     // 事务的三种状态-已提交
    private static final byte TRAN_STATUS_ABORTED = 2;      // 事务的三种状态-已回滚
    public static final long SUPER_TRANSACTION = 0;       // 超级事务，永远为committed状态
    public static final String XID_FILE_SUFFIX = ".tran";     // XID 文件后缀

    private RandomAccessFile raf;
    private FileChannel fc;
    private long transactionCounter;
    private ReentrantLock counterLock;

    public TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXidFile();
    }

    @Override
    public long begin() {
        counterLock.lock();
        try{
            long transaction = this.transactionCounter + 1;
            updateTransactionStatus(transaction, TRAN_STATUS_ACTIVE);
            incrTransactionCounter();
            return transaction;
        } finally {
            counterLock.unlock();
        }
    }

    @Override
    public void commit(long transaction) {
        updateTransactionStatus(transaction, TRAN_STATUS_COMMITTED);
    }

    @Override
    public void abort(long transaction) {
        updateTransactionStatus(transaction, TRAN_STATUS_ABORTED);
    }

    @Override
    public boolean isActive(long transaction) {
        if (transaction == SUPER_TRANSACTION) return false;
        return checkTransactionStatus(transaction, TRAN_STATUS_ACTIVE);
    }

    @Override
    public boolean isCommitted(long transaction) {
        if (transaction == SUPER_TRANSACTION) return true;
        return checkTransactionStatus(transaction, TRAN_STATUS_COMMITTED);
    }

    @Override
    public boolean isAborted(long transaction) {
        if (transaction == SUPER_TRANSACTION) return false;
        return checkTransactionStatus(transaction, TRAN_STATUS_ABORTED);
    }

    @Override
    public void close() {
        try{
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(new CloseFileException("close FileChannel or RandomAccessFile error"));
        }
    }

    /**
     * 检验xid文件是否合法，通过文件头8字节的数据（文件长度值）和真实文件长度比较，若相同则合法。
     */
    private void checkXidFile(){
        long fileLen = 0;
        try {
            fileLen = raf.length();
        } catch (IOException e) {
            Panic.panic(new XidFileBadException("get xidFile's length error"));
        }
        if (fileLen < XID_HEADER_LENGTH)
            Panic.panic(new XidFileBadException("xidFile's length < 8"));
        ByteBuffer buffer = ByteBuffer.allocate(XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(new XidFileBadException("select xidFile's head error"));
        }
        this.transactionCounter = Parser.byteArrayToLong(buffer.array());
        long end = getTransactionPosition(this.transactionCounter + 1);
        if (end != fileLen)
            Panic.panic(new XidFileBadException("xidFile's length != xidFile's head value"));
    }

    /**
     * 根据事务id获取该事务在xid文件里的偏移量
     * @param transaction 事务id
     * @return 该事务所在xid文件中的偏移量
     */
    private long getTransactionPosition(long transaction){
        return XID_HEADER_LENGTH + (transaction - 1) * TRANSACTION_SIZE;
    }

    /**
     * 修改所指定事务的状态
     * @param transaction 事务id
     * @param status 修改后的状态值
     */
    private void updateTransactionStatus(long transaction, byte status){
        long offset = getTransactionPosition(transaction);
        byte[] temp = new byte[TRANSACTION_SIZE];
        temp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(temp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(new UpdateXidFileException("write transaction's status error"));
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(new UpdateXidFileException("write transaction's status to disk error"));
        }
    }

    /**
     * 更新事务计数器，并更新xid事务文件头部文件长度
     */
    private void incrTransactionCounter(){
        this.transactionCounter++;
        ByteBuffer buffer = ByteBuffer.wrap(Parser.longToByteArray(transactionCounter));
        try {
            fc.position(0);
            fc.write(buffer);
        } catch (IOException e) {
            Panic.panic(new UpdateXidFileException("write xidFile's head error"));
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(new UpdateXidFileException("write xidFile's head to disk error"));
        }
    }

    /**
     * 检测事务是否处于某个特定的状态
     * @param transaction 事务id
     * @param status 事务的状态值
     * @return 是则返回true，反之返回false
     */
    private boolean checkTransactionStatus(long transaction, byte status){
        long offset = getTransactionPosition(transaction);
        ByteBuffer buffer = ByteBuffer.allocate(TRANSACTION_SIZE);
        try {
            fc.position(offset);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(new GetFromXidFileException("get transaction's status error"));
        }
        return buffer.array()[0] == status;
    }
}
