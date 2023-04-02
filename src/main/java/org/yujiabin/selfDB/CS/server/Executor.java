package org.yujiabin.selfDB.CS.server;

import org.yujiabin.selfDB.exception.NestedTransactionException;
import org.yujiabin.selfDB.exception.NoTransactionException;
import org.yujiabin.selfDB.table.parser.Parser;
import org.yujiabin.selfDB.table.vo.BeginRes;
import org.yujiabin.selfDB.table.TableManager;
import org.yujiabin.selfDB.table.parser.statement.*;

public class Executor {
    private long tran;
    public TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.tran = 0;
    }

    public void close() {
        if(tran != 0) {
            System.out.println("Abnormal Abort: " + tran);
            tbm.abort(tran);
        }
    }

    /**
     * 执行sql语句
     */
    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        Object stat = Parser.Parse(sql);
        if(stat instanceof Begin) {
            if(tran != 0) {
                throw new NestedTransactionException();
            }
            BeginRes r = tbm.begin((Begin)stat);
            tran = r.tran;
            return r.result;
        } else if(stat instanceof Commit) {
            if(tran == 0) {
                throw new NoTransactionException();
            }
            byte[] res = tbm.commit(tran);
            tran = 0;
            return res;
        } else if(stat instanceof Abort) {
            if(tran == 0) {
                throw new NoTransactionException();
            }
            byte[] res = tbm.abort(tran);
            tran = 0;
            return res;
        } else {
            return executeSQL(stat);
        }
    }

    private byte[] executeSQL(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if(tran == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            tran = r.tran;
        }
        try {
            byte[] res = null;
            if(stat instanceof Show) {
                res = tbm.show(tran);
            } else if(stat instanceof Create) {
                res = tbm.create(tran, (Create)stat);
            } else if(stat instanceof Select) {
                res = tbm.select(tran, (Select)stat);
            } else if(stat instanceof Insert) {
                res = tbm.insert(tran, (Insert)stat);
            } else if(stat instanceof Delete) {
                res = tbm.delete(tran, (Delete)stat);
            } else if(stat instanceof Update) {
                res = tbm.update(tran, (Update)stat);
            }
            return res;
        } catch(Exception e1) {
            e = e1;
            throw e;
        } finally {
            if(tmpTransaction) {
                if(e != null) {
                    tbm.abort(tran);
                } else {
                    tbm.commit(tran);
                }
                tran = 0;
            }
        }
    }
}
