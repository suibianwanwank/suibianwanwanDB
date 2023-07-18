package com.ccsu.metadata;

import com.google.common.collect.Maps;
import lombok.NonNull;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class DBTableSchema extends AbstractSchema implements Serializable {

    private String name;

    /**
     * table信息
     */
    private List<DBTable> tableList;

    public DBTableSchema(@NonNull String name, @NonNull List<DBTable> tableList) {
        this.name = name;
        this.tableList = tableList;
    }


    /**
     * 获取该schema中所有的表信息
     *
     * @return
     */
    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tableMap = Maps.newHashMap();
        for (DBTable table : this.tableList) {
            tableMap.put(table.getName(),table);
        }
        return tableMap;
    }
}
