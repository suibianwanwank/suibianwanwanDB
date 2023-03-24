package com.ccsu.vm;


public interface VersionManager {
    byte[] read(long xid, long uid,boolean isSnapshot) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;
    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid) throws Exception;
    void update(long xid, long uid,byte[] data) throws Exception;
}
