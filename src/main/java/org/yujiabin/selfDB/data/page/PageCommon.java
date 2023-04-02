package org.yujiabin.selfDB.data.page;

import org.yujiabin.selfDB.utils.Parser;

import java.util.Arrays;

public class PageCommon {

    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    /**
     * 获取空闲空间偏移量
     */
    public static short getFSO(byte[] raw){
        return Parser.byteArraysToShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 获取页空闲空间偏移量
     * @param page 页
     */
    public static short getFSO(Page page){
        return getFSO(page.getData());
    }

    /**
     * 设置页剩余空闲空间大小
     * @param raw 页数据
     * @param fso 空闲空间偏移量
     */
    public static void setFSO(byte[] raw, short fso){
        System.arraycopy(Parser.shortToByteArray(fso), 0, raw, OF_FREE, OF_DATA);
    }

    /**
     * 获取页剩余空闲空间的大小
     */
    public static int getFreeSpace(Page page){
        return PageCache.PAGE_SIZE - getFSO(page);
    }

    /**
     * 初始化页数据，即设置头部FSO
     * @return 初始化完后的数据
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    /**
     * 往页中添加数据
     * @param page 页
     * @param raw 数据
     * @return 添加数据的位置
     */
    public static short insert(Page page, byte[] raw){
        page.setDirty(true);
        short offset = getFSO(page);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        setFSO(page.getData(), (short) (offset + raw.length));
        return offset;
    }

    /**
     * 对Insert操作的恢复操作
     * @param pg 页
     * @param raw 数据
     * @param offset 所需插入数据的偏移量
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset+raw.length));
        }
    }

    /**
     * 对Update操作的恢复操作
     * @param pg 页
     * @param raw 数据
     * @param offset 所需修改数据的偏移量
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }

}
