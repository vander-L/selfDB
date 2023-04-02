package org.yujiabin.selfDB.utils;

import org.yujiabin.selfDB.exception.CreateFileException;
import org.yujiabin.selfDB.exception.FileReadWriteException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileUtil {
    public static RandomAccessFile getRandomAccessFile(File file){
        if (!file.canRead() || !file.canWrite())
            Panic.panic(new FileReadWriteException("the file can't select or write"));
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            Panic.panic(new FileNotFoundException("the file not found"));
        }
        return raf;
    }

    public static void createNewFile(File file){
        File parentFile = file.getParentFile();
        try {
            if (!parentFile.exists())
                parentFile.mkdirs();
            if (file.exists())
                file.delete();
            file.createNewFile();
        } catch (IOException e) {
            Panic.panic(new CreateFileException("createBtFile new file error"));
        }
    }
}
