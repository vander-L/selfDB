package org.yujiabin.selfDB.table;

import org.yujiabin.selfDB.data.DataManager;
import org.yujiabin.selfDB.exception.DuplicatedTableException;
import org.yujiabin.selfDB.exception.TableNotFoundException;
import org.yujiabin.selfDB.table.parser.statement.*;
import org.yujiabin.selfDB.table.vo.BeginRes;
import org.yujiabin.selfDB.utils.Parser;
import org.yujiabin.selfDB.version.VersionManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TableManagerImpl implements TableManager {
    VersionManager vm;
    DataManager dm;
    private Booter booter;
    private Map<String, Table> tableCache;
    private Map<Long, List<Table>> tranTableCache;
    private Lock lock;
    
    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.tranTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    private void loadTables() {
        long uid = firstTableUid();
        while(uid != 0) {
            Table tb = Table.loadTable(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }

    /**
     * 获取第一个表的uid
     */
    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.byteArrayToLong(raw);
    }

    /**
     * 修改第一个表的uid
     */
    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.longToByteArray(uid);
        booter.update(raw);
    }

    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead?1:0;
        res.tran = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }
    @Override
    public byte[] commit(long tran) throws Exception {
        vm.commit(tran);
        return "commit".getBytes();
    }
    @Override
    public byte[] abort(long tran) {
        vm.abort(tran);
        return "abort".getBytes();
    }
    @Override
    public byte[] show(long tran) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            List<Table> t = tranTableCache.get(tran);
            if(t == null) {
                return "\n".getBytes();
            }
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }
    @Override
    public byte[] create(long tran, Create create) throws Exception {
        lock.lock();
        try {
            if(tableCache.containsKey(create.tableName)) {
                throw new DuplicatedTableException();
            }
            Table table = Table.createTable(this, firstTableUid(), tran, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            if(!tranTableCache.containsKey(tran)) {
                tranTableCache.put(tran, new ArrayList<>());
            }
            tranTableCache.get(tran).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }
    @Override
    public byte[] insert(long tran, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if(table == null) {
            throw new TableNotFoundException();
        }
        table.insert(tran, insert);
        return "insert".getBytes();
    }
    @Override
    public byte[] select(long tran, Select select) throws Exception {
        lock.lock();
        Table table = tableCache.get(select.tableName);
        lock.unlock();
        if(table == null) {
            throw new TableNotFoundException();
        }
        return table.select(tran, select).getBytes();
    }
    @Override
    public byte[] update(long tran, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if(table == null) {
            throw new TableNotFoundException();
        }
        int count = table.update(tran, update);
        return ("update " + count).getBytes();
    }
    @Override
    public byte[] delete(long tran, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if(table == null) {
            throw new TableNotFoundException();
        }
        int count = table.delete(tran, delete);
        return ("delete " + count).getBytes();
    }
}
