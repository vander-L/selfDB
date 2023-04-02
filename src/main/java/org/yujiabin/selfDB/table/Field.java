package org.yujiabin.selfDB.table;

import com.google.common.primitives.Bytes;
import org.yujiabin.selfDB.exception.InvalidFieldException;
import org.yujiabin.selfDB.index.BPlusTree;
import org.yujiabin.selfDB.table.parser.statement.SingleExpression;
import org.yujiabin.selfDB.transaction.TransactionManagerImpl;
import org.yujiabin.selfDB.table.vo.FieldCalRes;
import org.yujiabin.selfDB.utils.Panic;
import org.yujiabin.selfDB.utils.vo.ParseStringRes;
import org.yujiabin.selfDB.utils.Parser;

import java.util.Arrays;
import java.util.List;

/**
 * field:[FieldName][TypeName][IndexUid]<br>
 * 如果field无索引，IndexUid为0<br>
 */
public class Field {
    long uid;
    private Table tb;
    String fieldName;
    String fieldType;
    private long index;
    private BPlusTree bt;

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_TRANSACTION, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseField(raw);
    }

    /**
     * 创建字段
     * @param tb 表
     * @param tran 事务
     * @param fieldName 字段名
     * @param fieldType 数据类型
     * @param indexed 有无索引
     */
    public static Field createField(Table tb, long tran, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        Field f = new Field(tb, fieldName, fieldType, 0);
        if(indexed) {
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        f.persistField(tran);
        return f;
    }

    /**
     * 判断字段有无索引
     */
    public boolean isIndexed() {
        return index != 0;
    }

    /**
     * 插入数据
     * @param key 数据的key
     * @param uid 数据的uid
     */
    public void insert(Object key, long uid) throws Exception {
        long uKey = valueToUid(key);
        bt.insert(uKey, uid);
    }

    /**
     * 查询数据key在[left,right]之间的数据
     */
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }

    public Object stringToValue(String str) {
        return switch (fieldType) {
            case "int32" -> Integer.parseInt(str);
            case "int64" -> Long.parseLong(str);
            case "string" -> str;
            default -> null;
        };
    }

    public long valueToUid(Object key) {
        long uid = 0;
        switch (fieldType) {
            case "string" -> uid = Parser.stringToUid((String) key);
            case "int32" -> {
                return (int) key;
            }
            case "int64" -> uid = (long) key;
        }
        return uid;
    }

    public byte[] valueToRaw(Object v) {
        return switch (fieldType) {
            case "int32" -> Parser.intToByteArray((int) v);
            case "int64" -> Parser.longToByteArray((long) v);
            case "string" -> Parser.stringToByteArray((String) v);
            default -> null;
        };
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch (fieldType) {
            case "int32" -> {
                res.v = Parser.byteArraysToInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
            }
            case "int64" -> {
                res.v = Parser.byteArrayToLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
            }
            case "string" -> {
                ParseStringRes r = Parser.byteArrayToString(raw);
                res.v = r.str;
                res.shift = r.next;
            }
        }
        return res;
    }

    public String valueToString(Object v) {
        return switch (fieldType) {
            case "int32" -> String.valueOf((int) v);
            case "int64" -> String.valueOf((long) v);
            case "string" -> (String) v;
            default -> null;
        };
    }

    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v;
        FieldCalRes res = new FieldCalRes();
        switch (exp.compareOp) {
            case "<" -> {
                res.left = 0;
                v = stringToValue(exp.value);
                res.right = valueToUid(v);
                if (res.right > 0) {
                    res.right--;
                }
            }
            case "=" -> {
                v = stringToValue(exp.value);
                res.left = valueToUid(v);
                res.right = res.left;
            }
            case ">" -> {
                res.right = Long.MAX_VALUE;
                v = stringToValue(exp.value);
                res.left = valueToUid(v) + 1;
            }
        }
        return res;
    }

    private Field parseField(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.byteArrayToString(raw);
        fieldName = res.str;
        position += res.next;
        res = Parser.byteArrayToString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        position += res.next;
        this.index = Parser.byteArrayToLong(Arrays.copyOfRange(raw, position, position+8));
        if(index != 0) {
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            } catch(Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    /**
     * 将字段持久化
     * @param tran 事务
     */
    private void persistField(long tran) throws Exception {
        byte[] nameRaw = Parser.stringToByteArray(fieldName);
        byte[] typeRaw = Parser.stringToByteArray(fieldType);
        byte[] indexRaw = Parser.longToByteArray(index);
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(tran, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    /**
     * 检测字段类型是否合法
     */
    private static void typeCheck(String fieldType) {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw new InvalidFieldException();
        }
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index!=0?", Index":", NoIndex")
                .append(")")
                .toString();
    }
}
