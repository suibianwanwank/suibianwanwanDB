package com.ccsu.metadata;

import com.ccsu.tb.Field;
import com.ccsu.tb.Table;
import lombok.Data;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
@Data
public class DBTable extends AbstractTable implements Serializable {

    /**
     * 表名称
     */
    private String name;

    /**
     * 列信息
     */
    private List<DBColume> columnList;

    public static DBTable of(Table table){
        DBTable dbTable = new DBTable();
        List<Field> fields = table.getFields();
        for(Field field:fields){
            dbTable.columnList.add(new DBColume(field.getFieldName(), field.getFieldType()));
        }
        return dbTable;
    }
    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        List<String> names = new ArrayList<>();
        List<RelDataType> types = new ArrayList<>();
        for (DBColume sqlExecuteColumn : columnList) {
            names.add(sqlExecuteColumn.getName());
            RelDataType sqlType = relDataTypeFactory.createSqlType(SqlTypeName.get(sqlExecuteColumn.getType().toUpperCase()));
            types.add(sqlType);
        }
        return relDataTypeFactory.createStructType(Pair.zip(names, types));
    }
}
