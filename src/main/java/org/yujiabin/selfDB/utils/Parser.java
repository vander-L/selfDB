package org.yujiabin.selfDB.utils;

import com.google.common.primitives.Bytes;
import org.yujiabin.selfDB.utils.vo.ParseStringRes;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Parser {


    public static byte[] shortToByteArray(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static byte[] intToByteArray(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static byte[] longToByteArray(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static short byteArraysToShort(byte[] buf){
        return ByteBuffer.wrap(buf, 0, 2).getShort();
    }

    public static int byteArraysToInt(byte[] buf){
        return ByteBuffer.wrap(buf, 0, 4).getInt();
    }

    public static long byteArrayToLong(byte[] buf){
        return ByteBuffer.wrap(buf, 0, 8).getLong();
    }

    public static long addressToUid(int pageNumber, short offset){
        return (long) pageNumber << 32 | offset;
    }

    public static ParseStringRes byteArrayToString(byte[] raw) {
        int length = byteArraysToInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4+length));
        return new ParseStringRes(str, length+4);
    }

    public static byte[] stringToByteArray(String str) {
        byte[] l = intToByteArray(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    public static long stringToUid(String key) {
        long seed = 13331;
        long res = 0;
        for(byte b : key.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }

}
