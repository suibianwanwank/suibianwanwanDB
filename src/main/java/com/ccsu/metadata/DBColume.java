package com.ccsu.metadata;

import lombok.Data;

@Data
public class DBColume {
    /**
     * 字段名称
     */
    private String name;

    /**
     * 字段类型名称
     */
    private String type;

    public DBColume(String name, String type) {
        this.name = name;
        this.type = type;
    }
}
