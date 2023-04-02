package org.yujiabin.selfDB.table;

import org.yujiabin.selfDB.exception.FileReadWriteException;
import org.yujiabin.selfDB.utils.FileUtil;
import org.yujiabin.selfDB.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * 记录第一个表的uid
 */
public class Booter {
    public static final String BOOTER_SUFFIX = ".bt";
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";
    String path;
    File file;

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    /**
     * 在指定路径创建.bt表文件
     */
    public static Booter createBtFile(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        FileUtil.createNewFile(f);
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(new FileReadWriteException());
        }
        return new Booter(path, f);
    }

    /**
     * 打开指定路径的.bt表文件
     */
    public static Booter openBtFile(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        if (!f.exists())
            Panic.panic(new FileNotFoundException("the file not found"));
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(new FileReadWriteException());
        }
        return new Booter(path, f);
    }

    /**
     * 删除指定位置的临时bt表文件
     */
    private static void removeBadTmp(String path) {
        new File(path+BOOTER_TMP_SUFFIX).delete();
    }

    /**
     * 获取文件的全部数据
     */
    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf;
    }

    /**
     * 修改.bt表文件的数据
     */
    public void update(byte[] data) {
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        FileUtil.createNewFile(tmp);
        if(!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(new FileReadWriteException());
        }
        try(FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch(IOException e) {
            Panic.panic(e);
        }
        try {
            Files.move(tmp.toPath(), new File(path+BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch(IOException e) {
            Panic.panic(e);
        }
        file = new File(path+BOOTER_SUFFIX);
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(new FileReadWriteException());
        }
    }

}
