package com.ccsu.dm;

public interface DataManager {

    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void update(long xid,long uid, byte[] data) throws Exception;

    void close();
}
