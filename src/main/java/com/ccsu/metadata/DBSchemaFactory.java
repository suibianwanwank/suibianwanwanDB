package com.ccsu.metadata;

import com.ccsu.tb.Table;
import com.ccsu.tb.TableManager;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.*;

public class DBSchemaFactory implements SchemaFactory {

    TableManager tableManager;
    /**
     * 创建schema
     * @param parentSchema
     * @param name
     * @param operand
     * @return
     */
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        List<DBTable> tableList = new ArrayList<>();
        List<Table> allTable = tableManager.getAllTable();
        allTable.forEach(table -> tableList.add(DBTable.of(table)));
        return new DBTableSchema(name,tableList);
    }
}
