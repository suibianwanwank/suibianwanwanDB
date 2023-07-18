package com.ccsu.metadata;


import com.ccsu.tb.TableManager;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

public class DBTableFactory implements TableFactory {
    TableManager tableManager;

    @Override
    public Table create(SchemaPlus schemaPlus, String name, Map map, RelDataType relDataType) {
        com.ccsu.tb.Table metaData= tableManager.getMetaData(name);
        return DBTable.of(metaData);
    }
}
