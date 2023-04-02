package org.yujiabin.selfDB.common;

import org.junit.Test;
import org.yujiabin.selfDB.exception.CacheFullException;
import org.yujiabin.selfDB.utils.Panic;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class CacheTest {

    static Random random = new SecureRandom();
    private CountDownLatch cdl;
    private MockCache cache;
    
    @Test
    public void testCache() {
        cache = new MockCache();
        cdl = new CountDownLatch(200);
        for(int i = 0; i < 200; i ++) {
            Runnable r = this::work;
            new Thread(r).start();
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void work() {
        for(int i = 0; i < 1000; i++) {
            long uid = random.nextInt();
            long h = 0;
            try {
                h = cache.getResources(uid);
            } catch (Exception e) {
                if(e instanceof CacheFullException) continue;
                Panic.panic(e);
            }
            assert h == uid;
            cache.releaseResource(h);
        }
        cdl.countDown();
    }
}
