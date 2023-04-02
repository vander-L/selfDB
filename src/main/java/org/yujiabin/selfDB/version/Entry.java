package org.yujiabin.selfDB.version;

import com.google.common.primitives.Bytes;
import org.yujiabin.selfDB.data.dataItem.DataItem;
import org.yujiabin.selfDB.utils.Parser;
import org.yujiabin.selfDB.utils.vo.SubArray;

import java.util.Arrays;

/**
 * Entry:[TranCreate][TranDelete][Data]
 */
public class Entry {
    private static final int OF_TRAN_CREATE = 0;
    private static final int OF_TRAN_DELETE = OF_TRAN_CREATE +8;
    private static final int OF_DATA = OF_TRAN_DELETE +8;
    private long uid;
    private DataItem di;
    private VersionManager vm;

    private Entry(VersionManager vm, DataItem di, long uid) {
        this.uid = uid;
        this.di = di;
        this.vm = vm;
    }

    /**
     * 通过版本和uid获取记录
     * @param vm 版本
     * @param uid uid
     */
    public static Entry getEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.getDataItem(uid);
        return new Entry(vm, di, uid);
    }

    public void remove() {
        di.release();
    }

    /**
     * 包装记录中的数据
     * @param tran 事务
     * @param data 数据
     */
    public static byte[] wrapEntryRaw(long tran, byte[] data) {
        byte[] updateTran = Parser.longToByteArray(tran);
        byte[] deleteTran = new byte[8];
        return Bytes.concat(updateTran, deleteTran, data);
    }

    /**
     * 获取记录中的数据
     */
    public byte[] getData() {
        di.readLock();
        try {
            SubArray sa = di.getData();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.arr, sa.start+OF_DATA, data, 0, data.length);
            return data;
        } finally {
            di.unReadLock();
        }
    }

    /**
     * 获取更新记录的事务
     */
    public long getTranUpdate() {
        di.readLock();
        try {
            SubArray sa = di.getData();
            return Parser.byteArrayToLong(Arrays.copyOfRange(sa.arr, sa.start+OF_TRAN_CREATE, sa.start+OF_TRAN_DELETE));
        } finally {
            di.unReadLock();
        }
    }

    /**
     * 获取删除记录的事务
     */
    public long getTranDelete() {
        di.readLock();
        try {
            SubArray sa = di.getData();
            return Parser.byteArrayToLong(Arrays.copyOfRange(sa.arr, sa.start+OF_TRAN_DELETE, sa.start+OF_DATA));
        } finally {
            di.unReadLock();
        }
    }

    /**
     * 设置删除记录的事务
     * @param tran 事务
     */
    public void setTranDelete(long tran) {
        di.beforeModify();
        try {
            SubArray sa = di.getData();
            System.arraycopy(Parser.longToByteArray(tran), 0, sa.arr, sa.start+OF_TRAN_DELETE, 8);
        } finally {
            di.afterModify(tran);
        }
    }

    public long getUid() {
        return uid;
    }

    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }
}
