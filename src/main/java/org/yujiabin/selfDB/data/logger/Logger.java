package org.yujiabin.selfDB.data.logger;

import org.yujiabin.selfDB.utils.Panic;
import org.yujiabin.selfDB.utils.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.yujiabin.selfDB.utils.FileUtil.createNewFile;
import static org.yujiabin.selfDB.utils.FileUtil.getRandomAccessFile;

/**
 * 日志文件格式：[XChecksum][Log1][Log2][Log3]...[LogN][BadTail]
 * 单条日志格式：[Size][Checksum][Data]
 */
public interface Logger {
    /**
     * 将数据写入日志
     */
    void log(byte[] data);

    /**
     * 截断日志
     */
    void truncateLog(long x) throws Exception;

    /**
     * 获取下一条日志
     */
    byte[] nextLog();

    /**
     * 将文件指针重新定位到第一条日志
     */
    void rewind();

    /**
     * 关闭Logger
     */
    void close();

    /**
     * 在指定路径创建log日志文件
     * @param path 文件路径
     */
    static Logger createLogFile(String path) {
        File file = new File(path+LoggerImpl.LOG_SUFFIX);
        createNewFile(file);
        RandomAccessFile raf = getRandomAccessFile(file);
        FileChannel fc = raf.getChannel();
        ByteBuffer buf = ByteBuffer.wrap(Parser.intToByteArray(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new LoggerImpl(raf, fc, 0);
    }

    /**
     * 打开指定位置的log日志文件
     * @param path 文件路径
     */
    static Logger openLogFile(String path) {
        File file = new File(path + LoggerImpl.LOG_SUFFIX);
        if (!file.exists())
            Panic.panic(new FileNotFoundException("the file not found"));
        RandomAccessFile raf = getRandomAccessFile(file);
        FileChannel fc = raf.getChannel();
        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();
        return lg;
    }
}
