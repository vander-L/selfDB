package org.yujiabin.selfDB.utils.vo;

/**
 * 共享内存数组对象
 */
public class SubArray {
    public byte[] arr;
    public int start;
    public int end;

    public SubArray(byte[] arr, int start, int end) {
        this.arr = arr;
        this.start = start;
        this.end = end;
    }
}
