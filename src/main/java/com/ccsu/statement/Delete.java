package com.ccsu.statement;

public class Delete {
    public String tableName;
    public Where2 where;

    @Override
    public String toString() {
        return "Delete{" +
                "tableName='" + tableName + '\'' +
                ", where=" + where +
                '}';
    }
}
