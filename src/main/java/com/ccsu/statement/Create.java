package com.ccsu.statement;

import java.util.Arrays;

public class Create {
    public String tableName;

    public String[] fieldName;

    public String[] fieldType;

    public String[] index;

    public String[] keyWord;

    @Override
    public String toString() {
        return "Create{" +
                "tableName='" + tableName + '\'' +
                ", fieldName=" + Arrays.toString(fieldName) +
                ", fieldType=" + Arrays.toString(fieldType) +
                ", index=" + Arrays.toString(index) +
                '}';
    }
}
