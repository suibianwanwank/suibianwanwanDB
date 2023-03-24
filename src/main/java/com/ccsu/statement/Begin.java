package com.ccsu.statement;

public class Begin {

    public final static int READ_UNCOMMITTED=0;
    public final static int READ_COMMITTED=1;
    public final static int REPEATABLE_READ=2;
    public byte isolationLevel;

}
