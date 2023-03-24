package com.ccsu.common;

public class Error {
    public static final Exception NullEntryException = new RuntimeException("Null entry!");

    public static final Exception WrongUpdateException= new RuntimeException("wrong update");
    public static final Exception DeadLockException= new RuntimeException("Dead Lock");

    public static final Exception InvalidCommandException = new RuntimeException("Invalid command!");

    public static final Exception ConcurrentUpdateException = new RuntimeException("Concurrent update issue!");

    public static final Exception TableNotFoundException = new RuntimeException("Table not found!");

    public static final Exception FieldNotFoundException = new RuntimeException("Field not found!");
    public static final Exception FileWriteException=new RuntimeException("File write wrong");
    public static final Exception FileExistsException=new RuntimeException("File has been exists");
    public static final Exception FileNotExistsException=new RuntimeException("File not exists");
    public static final Exception FileCannotRWException=new RuntimeException("File can not read or write");
    public static final Exception UndoBadLogFileException=new RuntimeException("Undo log has error");
    public static final Exception WriteFailException=new RuntimeException("Write failed");
    public static final Exception BadLogFileException=new RuntimeException("Bad Log");

}
