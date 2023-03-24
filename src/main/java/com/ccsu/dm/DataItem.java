package com.ccsu.dm;


import com.ccsu.page.Page;

public interface DataItem {







    byte[] data();
    void release() throws Exception;
    void lock();
    void unlock();
    void rLock();
    void runLock();

    Page page();

    long getUid();

    byte[] getRaw();

}
