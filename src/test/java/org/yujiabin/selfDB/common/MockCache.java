package org.yujiabin.selfDB.common;

public class MockCache extends AbstractCache<Long> {

    public MockCache() {
        super(50);
    }
    @Override
    protected Long getDataNotInCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseFromCache(Long obj) {

    }
}
