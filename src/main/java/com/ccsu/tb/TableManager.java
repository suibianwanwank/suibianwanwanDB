package com.ccsu.tb;

import com.ccsu.statement.*;

public interface TableManager {

    public byte[] show(long xid);
     byte[] create(long xid, Create create) throws Exception;

    public byte[] read(long xid, Select read) throws Exception;

    public byte[] insert(long xid, Insert insert) throws Exception;

    public long begin(Begin begin);


    public byte[] commit(long xid) throws Exception;


    byte[] rollback(long xid) throws Exception;

    byte[] update(long xid,Update update) throws Exception;

    public byte[] delete(long xid, Delete delete) throws Exception;

}
