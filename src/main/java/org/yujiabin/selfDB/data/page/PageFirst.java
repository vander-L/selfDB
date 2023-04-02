package org.yujiabin.selfDB.data.page;

import org.yujiabin.selfDB.utils.RandomUtil;

import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck
 * 数据库启动时给100~107字节处填入一个随机字节，关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public class PageFirst {

    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    /**
     * 开机时初始化第一页中的随机字符
     */
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    /**
     * 开机时初始化第一页中的随机字符
     */
    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomByteArray(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    /**
     * 正常关闭数据库时，拷贝开机时生成的随机字符串
     */
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    /**
     * 正常关闭数据库时，拷贝开机时生成的随机字符串
     */
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    /**
     * 检查两段随机字符串是否一样,就可以判断是否是正常关闭还是异常关闭
     */
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    /**
     * 检查两段随机字符串是否一样,就可以判断是否是正常关闭还是异常关闭
     */
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC), Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC +2* LEN_VC));
    }
}
