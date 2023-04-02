package org.yujiabin.selfDB.table;

import org.yujiabin.selfDB.data.DataManager;
import org.yujiabin.selfDB.table.parser.statement.*;
import org.yujiabin.selfDB.table.vo.BeginRes;
import org.yujiabin.selfDB.utils.Parser;
import org.yujiabin.selfDB.version.VersionManager;

public interface TableManager {
    BeginRes begin(Begin begin);
    byte[] commit(long tran) throws Exception;
    byte[] abort(long tran);

    byte[] show(long tran);
    byte[] create(long tran, Create create) throws Exception;

    byte[] insert(long tran, Insert insert) throws Exception;
    byte[] select(long tran, Select select) throws Exception;
    byte[] update(long tran, Update update) throws Exception;
    byte[] delete(long tran, Delete delete) throws Exception;

    static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.createBtFile(path);
        booter.update(Parser.longToByteArray(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.openBtFile(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
