package com.ccsu.statement;

import java.util.Arrays;

public class Insert {

    public String tableName;

    public String[] values;

    public String[] filedName;


    @Override
    public String toString() {
        return "Insert{" +
                "tableName='" + tableName + '\'' +
                ", values=" + Arrays.toString(values) +
                ", filedName=" + Arrays.toString(filedName) +
                '}';
    }



}
