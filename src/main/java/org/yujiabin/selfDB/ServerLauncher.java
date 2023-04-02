package org.yujiabin.selfDB;

import org.apache.commons.cli.*;
import org.yujiabin.selfDB.CS.server.Server;
import org.yujiabin.selfDB.data.DataManager;
import org.yujiabin.selfDB.exception.InvalidMemException;
import org.yujiabin.selfDB.table.TableManager;
import org.yujiabin.selfDB.transaction.TransactionManager;
import org.yujiabin.selfDB.utils.Panic;
import org.yujiabin.selfDB.version.VersionManager;
import org.yujiabin.selfDB.version.VersionManagerImpl;

public class ServerLauncher {

    public static final int port = 9999;
    public static final long DEFAULT_MEM = (1<<20)*64;  //默认内存 2^20*64
    public static final long KB = 1 << 10;  //1024 2^10
    public static final long MB = 1 << 20;  //1048576 2^20
    public static final long GB = 1 << 30;  //1073741824 2^30

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("open" , "openBtFile", true, "-openBtFile DBPath");
        options.addOption("create", "createBtFile", true, "-createBtFile DBPath");
        options.addOption("mem", true, "-mem 64MB");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options,args);
        if(cmd.hasOption("open")) {
            openDatabase(cmd.getOptionValue("openBtFile"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if(cmd.hasOption("create")) {
            createDatabase(cmd.getOptionValue("createBtFile"));
            return;
        }
        System.out.println("Usage: launcher (openBtFile|createBtFile) DBPath");
    }

    private static void createDatabase(String path) {
        TransactionManager tm = TransactionManager.createXidFile(path);
        DataManager dm = DataManager.createFile(path, DEFAULT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDatabase(String path, long mem) {
        TransactionManager tm = TransactionManager.openXidFile(path);
        DataManager dm = DataManager.openFile(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    private static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFAULT_MEM;
        }
        if(memStr.length() < 2) {
            Panic.panic(new InvalidMemException());
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch (unit) {
            case "KB" -> {
                return memNum * KB;
            }
            case "MB" -> {
                return memNum * MB;
            }
            case "GB" -> {
                return memNum * GB;
            }
            default -> Panic.panic(new InvalidMemException());
        }
        return DEFAULT_MEM;
    }
}
