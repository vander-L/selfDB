package org.yujiabin.selfDB.table.parser;

import org.yujiabin.selfDB.exception.InvalidCommandException;
import org.yujiabin.selfDB.table.parser.statement.*;

import java.util.ArrayList;
import java.util.List;

/**
 * begin statement <br>
 *     begin [isolation level (read committed,repeatable read)] <br>
 *         begin isolation level read committed <p></p>
 *
 * commit statement<br>
 *     commit <p></p>
 *
 * abort statement<br>
 *     abort<p></p>
 *
 * createBtFile statement<br>
 *     createBtFile table {table name} <br>
 *     {field name} {field type} <br>
 *     {field name} {field type} <br>
 *     ...<br>
 *     {field name} {field type}<br>
 *     [(index {field name list})]<br>
 *         createBtFile table students<br>
 *         id int32,<br>
 *         name string,<br>
 *         age int32,<br>
 *         (index id name)<p></p>
 *
 * drop statement <br>
 *     drop table {table name} <br>
 *         drop table students <p></p>
 *
 * select statement <br>
 *     select (*{field name list}) from {table name} [{where statement}]<br>
 *         select * from student where id = 1<br>
 *         select name from student where id > 1 and id < 4<br>
 *         select name, age, id from student where id = 12<p></p>
 *
 * insert statement<br>
 *     insert into {table name} values {value list} <br>
 *         insert into student values 5 "connery" 22 <p></p>
 *
 * delete statement<br>
 *     delete from {table name} {where statement}<br>
 *         delete from student where name = "connery"<p></p>
 *
 * update statement<br>
 *     update {table name} set {field name}={value} [{where statement}]<br>
 *         update student set name = "hans" where id = 5<p></p>
 *
 * where statement <br>
 *     where {field name} (><=) {value} [(andor) {field name} (><=) {value}]<br>
 *         where age > 10 or age < 3 <p></p>
 *
 * {field name} {table name} <br>
 *     [a-zA-Z][a-zA-Z0-9_]* <p></p>
 *
 * {field type}<br>
 *     int32 int64 string<p></p>
 *
 * {value}<br>
 *     .*
 */
public class Parser {
    /**
     * 将sql语句转换为系统可识别的字节数组
     */
    public static Object Parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek();
        tokenizer.pop();

        Object stat = null;
        Exception statErr = null;
        try {
            stat = switch (token) {
                case "begin" -> parseBegin(tokenizer);
                case "commit" -> parseCommit(tokenizer);
                case "abort" -> parseAbort(tokenizer);
                case "create" -> parseCreate(tokenizer);
                case "drop" -> parseDrop(tokenizer);
                case "select" -> parseSelect(tokenizer);
                case "insert" -> parseInsert(tokenizer);
                case "delete" -> parseDelete(tokenizer);
                case "update" -> parseUpdate(tokenizer);
                case "show" -> parseShow(tokenizer);
                default -> throw new InvalidCommandException();
            };
        } catch(Exception e) {
            statErr = e;
        }
        try {
            String next = tokenizer.peek();
            if(!"".equals(next)) {
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch(Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }
        if(statErr != null) {
            throw statErr;
        }
        return stat;
    }

    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            return new Show();
        }
        throw new InvalidCommandException();
    }

    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();
        update.tableName = tokenizer.peek();
        tokenizer.pop();

        if(!"set".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }
        tokenizer.pop();

        update.fieldName = tokenizer.peek();
        tokenizer.pop();

        if(!"=".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }
        tokenizer.pop();

        update.value = tokenizer.peek();
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            update.where = null;
            return update;
        }

        update.where = parseWhere(tokenizer);
        return update;
    }

    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();
        if(!"from".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw new InvalidCommandException();
        }
        delete.tableName = tableName;
        tokenizer.pop();

        delete.where = parseWhere(tokenizer);
        return delete;
    }

    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();
        if(!"into".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw new InvalidCommandException();
        }
        insert.tableName = tableName;
        tokenizer.pop();

        if(!"values".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }

        List<String> values = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if("".equals(value)) {
                break;
            } else {
                values.add(value);
            }
        }
        insert.values = values.toArray(new String[values.size()]);

        return insert;
    }

    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();
        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        if("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            while(true) {
                String field = tokenizer.peek();
                if(!isName(field)) {
                    throw new InvalidCommandException();
                }
                fields.add(field);
                tokenizer.pop();
                if(",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        read.fields = fields.toArray(new String[fields.size()]);

        if(!"from".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw new InvalidCommandException();
        }
        read.tableName = tableName;
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            read.where = null;
            return read;
        }

        read.where = parseWhere(tokenizer);
        return read;
    }

    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        if(!"where".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }
        tokenizer.pop();

        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;

        String logicOp = tokenizer.peek();
        if("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }
        if(!isLogicOp(logicOp)) {
            throw new InvalidCommandException();
        }
        where.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;

        if(!"".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }
        return where;
    }

    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();
        
        String field = tokenizer.peek();
        if(!isName(field)) {
            throw new InvalidCommandException();
        }
        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if(!isCmpOp(op)) {
            throw new InvalidCommandException();
        }
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }

    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw new InvalidCommandException();
        }
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }
        
        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    private static Create parseCreate(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }
        tokenizer.pop();

        Create create = new Create();
        String name = tokenizer.peek();
        if(!isName(name)) {
            throw new InvalidCommandException();
        }
        create.tableName = name;

        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if("(".equals(field)) {
                break;
            }

            if(!isName(field)) {
                throw new InvalidCommandException();
            }

            tokenizer.pop();
            String fieldType = tokenizer.peek();
            if(!isType(fieldType)) {
                throw new InvalidCommandException();
            }
            fNames.add(field);
            fTypes.add(fieldType);
            tokenizer.pop();
            
            String next = tokenizer.peek();
            if(",".equals(next)) {
                continue;
            } else if("".equals(next)) {
                throw new InvalidCommandException();
            } else if("(".equals(next)) {
                break;
            } else {
                throw new InvalidCommandException();
            }
        }
        create.fieldName = fNames.toArray(new String[fNames.size()]);
        create.fieldType = fTypes.toArray(new String[fTypes.size()]);

        tokenizer.pop();
        if(!"index".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }

        List<String> indexes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if(")".equals(field)) {
                break;
            }
            if(!isName(field)) {
                throw new InvalidCommandException();
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[indexes.size()]);
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }
        return create;
    }

    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }
        return new Abort();
    }

    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {
            throw new InvalidCommandException();
        }
        return new Commit();
    }

    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek();
        Begin begin = new Begin();
        if("".equals(isolation)) {
            return begin;
        }
        if(!"isolation".equals(isolation)) {
            throw new InvalidCommandException();
        }

        tokenizer.pop();
        String level = tokenizer.peek();
        if(!"level".equals(level)) {
            throw new InvalidCommandException();
        }
        tokenizer.pop();

        String tmp1 = tokenizer.peek();
        if("select".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("committed".equals(tmp2)) {
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {
                    throw new InvalidCommandException();
                }
                return begin;
            } else {
                throw new InvalidCommandException();
            }
        } else if("repeatable".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("select".equals(tmp2)) {
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {
                    throw new InvalidCommandException();
                }
                return begin;
            } else {
                throw new InvalidCommandException();
            }
        } else {
            throw new InvalidCommandException();
        }
    }

    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

    private static boolean isType(String tp) {
        return ("int32".equals(tp) || "int64".equals(tp) ||
                "string".equals(tp));
    }

    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }
}
