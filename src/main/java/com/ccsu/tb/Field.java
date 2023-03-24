package com.ccsu.tb;
import com.ccsu.bp.BPlusTree;
import com.ccsu.common.Error;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.tp.Type;
import com.ccsu.utils.Bytes;
import com.ccsu.utils.ParseStringRes;
import com.ccsu.utils.Parser;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;

/**
 * field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果field无索引，IndexUid为0
 */
@Slf4j
public class Field {
    Long uid;
    String fieldName;
    String fieldType;
    private long index;
    Table tb;
    BPlusTree bt;

    public Field(String fieldName, String fieldType, long index,Table tb) {
        this.fieldName=fieldName;
        this.fieldType=fieldType;
        this.index=index;
        this.tb=tb;
    }

    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {

            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid,true);

        } catch (Exception e) {
            log.error("读取失败");
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);
    }

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        fieldName = res.str;
        position += res.next;
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        position += res.next;
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        if(index != 0) {
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm,fieldType);
            } catch(Exception e) {

            }
        }
        return this;
    }

    public boolean isIndex(){
        return bt!=null;
    }
    public static Field createField(Table tb,long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        Field f = new Field(fieldName, fieldType, 0,tb);
        if(indexed) {
            long index = BPlusTree.create(tb.dm);
            BPlusTree bt=BPlusTree.load(index,tb.dm,fieldType);
            f.index=index;
            f.bt=bt;
        }
        f.unParseSelf(xid);

        return f;
    }

    private static void typeCheck(String fieldType) throws Exception {
       //TODO 做类型检查
    }


    private void unParseSelf(long xid) throws Exception {
        byte[] bytes = Parser.unParseString(fieldName);
        byte[] bytes1 = Parser.unParseString(fieldType);
        byte[] bytes2 = Parser.unParseLong(index);
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(bytes, bytes1, bytes2));

    }

    public String printValue(Type t) {

        return t.parseString();
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index!=0?", Index":", NoIndex")
                .append(")")
                .toString();
    }

    public List<Long> search(Type left, Type right) throws Exception {
        if(bt!=null){
            return bt.searchRange(left,right);
        }

        throw Error.FieldNotFoundException;

    }


}
