package com.ccsu.statement;

import java.util.Arrays;

public class  Select {
    public String tableName;
    public String[] fields;
    public Where2 where;

    @Override
    public String toString() {
        return "Select{" +
                "tableName='" + tableName + '\'' +
                ", fields=" + Arrays.toString(fields) +
                ", where=" + where +
                '}';
    }
}
