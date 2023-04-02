package org.yujiabin.selfDB.data.logger;

import com.google.common.primitives.Bytes;
import org.yujiabin.selfDB.exception.BadLogFileException;
import org.yujiabin.selfDB.utils.Panic;
import org.yujiabin.selfDB.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LoggerImpl implements Logger{

    private static final int SEED = 13331;
    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    //size4位,Checksum4位,data从第八位开始
    private static final int OF_DATA = OF_CHECKSUM + 4;
    public static final String LOG_SUFFIX = ".log";
    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock lock;
    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int allChecksum;

    public LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile raf, FileChannel fc, int allChecksum) {
        this.raf = raf;
        this.fc = fc;
        this.allChecksum = allChecksum;
        lock = new ReentrantLock();
    }

    /**
     * 打开日志文件时，对文件的读取和检查操作
     */
    public void init(){
        long size = 0;
        try{
            size = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size < 4)
            Panic.panic(new BadLogFileException());
        ByteBuffer buf = ByteBuffer.allocate(4);
        try{
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        allChecksum = Parser.byteArraysToInt(buf.array());
        fileSize = size;
        checkAndRemoveBadTail();
    }

    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try{
            fc.position(fc.size());
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
        updateAllChecksum(log);
    }

    @Override
    public void truncateLog(long x) throws Exception {
        lock.lock();
        try{
            fc.truncate(x);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] nextLog() {
        lock.lock();
        try{
            byte[] log = internNext();
            if (log == null)
                return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try{
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 获取一条日志的检验和
     * @param checksum 检验和初始参数
     * @param log 单条日志
     * @return 该日志的检验和
     */
    private int getOneChecksum(int checksum, byte[] log){
        for (byte b : log)
            checksum = checksum * SEED + b;
        return checksum;
    }

    /**
     * 方法使用迭代器模式，获取下一条日志
     */
    private byte[] internNext(){
        if (position + OF_DATA >= fileSize)
            return null;
        ByteBuffer temp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(temp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.byteArraysToInt(temp.array());
        if (position + size + OF_DATA > fileSize)
            return null;
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try{
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int checksum1 = getOneChecksum(0, Arrays.copyOfRange(buf.array(), OF_DATA, buf.array().length));
        int checksum2 = Parser.byteArraysToInt(Arrays.copyOfRange(buf.array(), OF_CHECKSUM, OF_DATA));
        if (checksum1 != checksum2)
            return null;
        position += buf.array().length;
        return buf.array();
    }

    /**
     * 检查log日志文件是否含有badTail并将其移除
     */
    private void checkAndRemoveBadTail(){
        rewind();
        int allChecksumTemp = 0;
        while (true){
            byte[] log = internNext();
            if (log == null) break;
            allChecksumTemp =getOneChecksum(allChecksumTemp, log);
        }
        if (allChecksumTemp != allChecksum)
            Panic.panic(new BadLogFileException());
        try {
            truncateLog(position);
            raf.seek(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        rewind();
    }

    /**
     * 将数据包装成一条日志
     */
    private byte[] wrapLog(byte[] data){
        return Bytes.concat(
                Parser.intToByteArray(data.length),
                Parser.intToByteArray(getOneChecksum(0, data)),
                data
        );
    }

    /**
     * 更新log日志文件的检验和
     * @param log 单条日志
     */
    private void updateAllChecksum(byte[] log){
        allChecksum = getOneChecksum(allChecksum, log);
        try{
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.intToByteArray(allChecksum)));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
