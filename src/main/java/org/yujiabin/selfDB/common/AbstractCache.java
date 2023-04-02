package org.yujiabin.selfDB.common;

import org.yujiabin.selfDB.exception.CacheFullException;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache<T> {

    private HashMap<Long, T> cacheData;             // 实际缓存的数据
    private HashMap<Long, Integer> resourceCounts;          // 资源的引用个数
    private HashMap<Long, Boolean> resourceGetting;         // 正在被获取的资源

    private int maxNumOfResource;       //缓存最大资源数
    private int numOfResource = 0;      //缓存元素个数
    private Lock lock;

    public AbstractCache(int maxNumOfResource) {
        this.maxNumOfResource = maxNumOfResource;
        cacheData = new HashMap<>();
        resourceCounts = new HashMap<>();
        resourceGetting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 当资源不在缓存中时，获取资源
     * @param key 资源的key
     * @return 所获取的数据
     */
    protected abstract T getDataNotInCache(long key) throws Exception;

    /**
     * 写回缓存中的资源
     * @param obj 需写回的数据数据
     */
    protected abstract void releaseFromCache(T obj);

    /**
     * 从缓存中获取资源，若不在缓存中，则先放入缓存中
     * @param key 资源的key
     * @return 数据
     */
    protected T getResources(long key) throws Exception{
        while (true){
            lock.lock();
            //如果正在获取资源
            if (resourceGetting.containsKey(key)){
                lock.unlock();
                TimeUnit.SECONDS.sleep(1);
                continue;
            }
            //如果缓存中有该资源
            if (cacheData.containsKey(key)){
                T obj = cacheData.get(key);
                resourceCounts.put(key, resourceCounts.get(key) + 1);
                lock.unlock();
                return obj;
            }
            //如果缓存已经满了
            if (maxNumOfResource > 0 && numOfResource == maxNumOfResource){
                lock.unlock();
                throw new CacheFullException("the cache is full");
            }
            //缓存中没有数据时，将资源数+1，并设置为正在获取
            numOfResource++;
            resourceGetting.put(key, true);
            lock.unlock();
            break;
        }
        //从表中获取数据（缓存中没有）
        T obj;
        try {
            obj = getDataNotInCache(key);
        } catch (Exception e) {
            //从表中获取数据失败时，将缓存资源数变为旧值
            lock.lock();
            try {
                numOfResource--;
                resourceGetting.remove(key);
            } finally {
                lock.unlock();
            }
            throw e;
        }
        //将资源放入缓存
        lock.lock();
        try {
            resourceGetting.remove(key);
            cacheData.put(key, obj);
            resourceCounts.put(key, 1);
        } finally {
            lock.unlock();
        }
        return obj;
    }

    /**
     * 释放一个资源
     * @param key 资源的key
     */
    protected void releaseResource(long key){
        lock.lock();
        try{
            int ref = resourceCounts.get(key) - 1;
            if (ref == 0){
                releaseFromCache(cacheData.get(key));
                resourceCounts.remove(key);
                cacheData.remove(key);
                numOfResource--;
            }else {
                resourceCounts.put(key, ref);
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，释放所有资源
     */
    protected void close(){
        lock.lock();
        try {
            for (long key : cacheData.keySet()) {
                T obj = cacheData.get(key);
                releaseFromCache(obj);
                resourceCounts.remove(key);
                cacheData.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }
}
