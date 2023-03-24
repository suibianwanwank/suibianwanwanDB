package com.ccsu.tm;

public interface TransactionManager {

    long begin();

    void commit(long xid) throws Exception;

    void abort(long xid) throws Exception;

    boolean isActive(long xid);

    boolean isCommitted(long xid);

    boolean isAborted(long xid);

    void close();

    long getUid(long xid) throws Exception;

    void setUid(long xid,long uid) throws Exception;

}
