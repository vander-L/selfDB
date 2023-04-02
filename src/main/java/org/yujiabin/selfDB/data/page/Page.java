package org.yujiabin.selfDB.data.page;

public interface Page {
    /**
     * 对页进行操作前的加锁操作
     */
    void lock();

    /**
     * 对页进行完操作的解锁操作
     */
    void unlock();

    /**
     * 将该页面从内存中释放
     */
    void release();

    /**
     * 修改页面是否为脏页面
     */
    void setDirty(boolean dirty);

    /**
     * 判断页面是否为脏页面
     */
    boolean isDirty();

    /**
     * 获取当前页的页号
     * @return 页号
     */
    int getPageNumber();

    /**
     * 获取该页的数据
     * @return 数据
     */
    byte[] getData();
}