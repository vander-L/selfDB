package org.yujiabin.selfDB.data;

import org.junit.Test;
import org.yujiabin.selfDB.data.dataItem.DataItem;
import org.yujiabin.selfDB.data.page.PageCache;
import org.yujiabin.selfDB.transaction.MockTransactionManager;
import org.yujiabin.selfDB.transaction.TransactionManager;
import org.yujiabin.selfDB.utils.Panic;
import org.yujiabin.selfDB.utils.RandomUtil;
import org.yujiabin.selfDB.utils.vo.SubArray;

import java.io.File;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataManagerTest {

    static List<Long> uids0, uids1;
    static Lock uidsLock;

    static Random random = new SecureRandom();

    private void initUids() {
        uids0 = new ArrayList<>();
        uids1 = new ArrayList<>();
        uidsLock = new ReentrantLock();
    }

    private void worker(DataManager dm0, DataManager dm1, int tasksNum, int insertRation, CountDownLatch cdl) {
        int dataLen = 10;
        try {
            for(int i = 0; i < tasksNum; i ++) {
                int op = Math.abs(random.nextInt()) % 100;
                if(op < insertRation) {
                    byte[] data = RandomUtil.randomByteArray(dataLen);
                    long u0, u1 = 0;
                    try {
                        u0 = dm0.insertDataItem(0, data);
                    } catch (Exception e) {
                        continue;
                    }
                    try {
                        u1 = dm1.insertDataItem(0, data);
                    } catch(Exception e) {
                        Panic.panic(e);
                    }
                    uidsLock.lock();
                    uids0.add(u0);
                    uids1.add(u1);
                    uidsLock.unlock();
                } else {
                    uidsLock.lock();
                    if(uids0.size() == 0) {
                        uidsLock.unlock();
                        continue;
                    }
                    int tmp = Math.abs(random.nextInt()) % uids0.size();
                    long u0 = uids0.get(tmp);
                    long u1 = uids1.get(tmp);
                    DataItem data0 = null, data1 = null;
                    try {
                        data0 = dm0.getDataItem(u0);
                    } catch (Exception e) {
                        Panic.panic(e);
                        continue;
                    }
                    if(data0 == null) continue;
                    try {
                        data1 = dm1.getDataItem(u1);
                    } catch (Exception e) {}

                    data0.readLock(); data1.readLock();
                    SubArray s0 = data0.getData(); SubArray s1 = data1.getData();
                    assert Arrays.equals(Arrays.copyOfRange(s0.arr, s0.start, s0.end), Arrays.copyOfRange(s1.arr, s1.start, s1.end));
                    data0.unReadLock(); data1.unReadLock();

                    byte[] newData = RandomUtil.randomByteArray(dataLen);
                    data0.beforeModify(); data1.beforeModify();
                    System.arraycopy(newData, 0, s0.arr, s0.start, dataLen);
                    System.arraycopy(newData, 0, s1.arr, s1.start, dataLen);
                    data0.afterModify(0); data1.afterModify(0);
                    data0.release(); data1.release();
                }
            }
        } finally {
            cdl.countDown();
        }
    }
    
    @Test
    public void testDMSingle() throws Exception {
        TransactionManager tm0 = new MockTransactionManager();
        DataManager dm0 = DataManager.createFile("/tmp/TESTDMSingle", PageCache.PAGE_SIZE*10, tm0);
        DataManager mdm = MockDataManager.newMockDataManager();

        int tasksNum = 10000;
        CountDownLatch cdl = new CountDownLatch(1);
        initUids();
        Runnable r = () -> worker(dm0, mdm, tasksNum, 50, cdl);
        new Thread(r).run();
        cdl.await();
        dm0.close(); mdm.close();

        new File("/tmp/TESTDMSingle.db").delete();
        new File("/tmp/TESTDMSingle.log").delete();
    }

    @Test
    public void testDMMulti() throws InterruptedException {
        TransactionManager tm0 = new MockTransactionManager();
        DataManager dm0 = DataManager.createFile("/tmp/TestDMMulti", PageCache.PAGE_SIZE*10, tm0);
        DataManager mdm = MockDataManager.newMockDataManager();

        int tasksNum = 500;
        CountDownLatch cdl = new CountDownLatch(10);
        initUids();
        for(int i = 0; i < 10; i ++) {
            Runnable r = () -> worker(dm0, mdm, tasksNum, 50, cdl);
            new Thread(r).run();
        }
        cdl.await();
        dm0.close(); mdm.close();

        new File("/tmp/TestDMMulti.db").delete();
        new File("/tmp/TestDMMulti.log").delete();
    }

    @Test
    public void testRecoverySimple() throws InterruptedException {
        TransactionManager tm0 = TransactionManager.createXidFile("/tmp/TestRecoverySimple");
        DataManager dm0 = DataManager.createFile("/tmp/TestRecoverySimple", PageCache.PAGE_SIZE*30, tm0);
        DataManager mdm = MockDataManager.newMockDataManager();
        dm0.close();

        initUids();
        int workerNums = 10;
        for(int i = 0; i < 8; i ++) {
            dm0 = DataManager.openFile("/tmp/TestRecoverySimple", PageCache.PAGE_SIZE*10, tm0);
            CountDownLatch cdl = new CountDownLatch(workerNums);
            for(int k = 0; k < workerNums; k ++) {
                final DataManager dm = dm0;
                Runnable r = () -> worker(dm, mdm, 100, 50, cdl);
                new Thread(r).run();
            }
            cdl.await();
        }
        dm0.close(); mdm.close();

        new File("/tmp/TestRecoverySimple.db").delete();
        new File("/tmp/TestRecoverySimple.log").delete();
        new File("/tmp/TestRecoverySimple.tran").delete();

    }
}
