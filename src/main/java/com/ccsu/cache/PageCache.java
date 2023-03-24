package com.ccsu.cache;

import com.ccsu.page.Page;

public interface PageCache {


    int newPage(byte[] initData);

    Page getPage(int pgNo) throws Exception;

    void close();

    void release(Page page);

    int getPageNumber();

    void flushPage(Page pg);

    void truncateByBgno(int maxPgno);
}
