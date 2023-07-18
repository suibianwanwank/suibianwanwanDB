package com.ccsu.tb;
import com.alibaba.fastjson.annotation.JSONField;
import com.ccsu.common.Error;
import com.ccsu.dm.DataItem;
import com.ccsu.dm.DataManager;
import com.ccsu.statement.*;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.tp.Type;
import com.ccsu.tp.TypeFactory;
import com.ccsu.utils.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 */
@Slf4j
public class Table {
    TableManager tbm;

    long uid;

    String name;

//    byte status;

    long nextUid;

    List<Field> fields = new ArrayList<>();

    Map<String,Field> fieldBaseMessage=new HashMap<>();

    DataManager dm;

    public Table( TableManager tbm,String tableName, long nextUid,DataManager dm) {
        this.tbm=tbm;
        this.name = tableName;
        this.nextUid = nextUid;
        this.dm=dm;
    }

    public  static Table createTable(TableManager tbm,long nextUid, long xid, Create create,DataManager dm) throws Exception {
        Table tb=new Table(tbm,create.tableName,nextUid,dm);
        for (int i=0; i<create.fieldName.length; i++) {
            String fieldName=create.fieldName[i];
            String fieldType=create.fieldType[i];

            boolean indexed = false;
            for(int j = 0; j < create.index.length; j ++) {
                if(fieldName.equals(create.index[j])) {
                    indexed = true;
                    log.info("fielname{}",fieldName);
                    break;
                }
            }
            Field field = Field.createField(tb, xid, fieldName, fieldType, indexed);
            tb.fields.add(field);
            tb.fieldBaseMessage.put(fieldName,field);
        }
        return tb.unParseSelf(xid);
    }


    private byte[] entry2Raw(Map<String, Type> entry) {
        byte[] bytes=new byte[0];
        for (Field field : fields) {
            Type type = entry.get(field.fieldName);
            bytes=Bytes.concat(bytes,type.parseRaw());
        }
        return bytes;
    }

    public void insert(long xid, Insert insert) throws Exception {
        byte[] bytes=new byte[0];
        for (int i=0;i<fields.size();i++) {
            Type type = TypeFactory.unParseString(fields.get(i).fieldType, insert.values[i]);
            bytes=Bytes.concat(bytes,type.parseRaw());
        }
        long uid = ((TableManagerImpl)tbm).vm.insert(xid, bytes);

        for (int i=0;i<fields.size();i++) {

            Type type = TypeFactory.unParseString(fields.get(i).fieldType, insert.values[i]);
            if(fields.get(i).bt!=null){
                fields.get(i).bt.insert(type,uid);
            }
        }
    }



    private Table unParseSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.unParseString(name);
        byte[] nextRaw = Parser.unParseLong(nextUid);
        byte[] fieldRaw = new byte[0];

        for(Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.unParseLong(field.uid));
        }
        uid = dm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }
    public Table(TableManager tbm, long uid,DataManager dm) {
        this.tbm = tbm;
        this.uid = uid;
        this.dm=dm;
    }

    public static Table loadTable(TableManagerImpl tbm, long uid,DataManager dm) {
        byte[] raw = null;
        try {
            byte[] data = tbm.dm.read(uid).data();
            raw=Arrays.copyOfRange(data,0,data.length);
        } catch (Exception e) {
            Panic.panic(e);
        }
        Table tb=new Table(tbm,uid,dm);
        //解析raw返回table
        return  tb.parseSelf(raw);
    }

    /**
     * Convert byte arrays read from disk to tables
     * @param raw
     * @return
     */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        position += 8;

        while(position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
            position += 8;
            Field field = Field.loadField(this, uid);
            fields.add(field);
            fieldBaseMessage.put(field.fieldName,field);
        }
        return this;
    }

    public String read(long xid, Select select) throws Exception {
        StringBuilder sb = new StringBuilder();
        List<Long> uids=null;
        uids=parseWhere2(xid,select.where);

        for (Long uid : uids) {
            byte[] data = ((TableManagerImpl) tbm).vm.read(xid, uid,true);
            if(data == null) continue;
            Map<String, Type> entry = parseEntry(data,select.fields);
            sb.append(printEntry(entry)).append("\n");
        }

        return sb.toString();
    }

    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere2(xid,delete.where);
        int count = 0;
        for (Long uid : uids) {
            if(((TableManagerImpl)tbm).vm.delete(xid, uid)) {
                count ++;
            }
        }
        return count;
    }

    /**
     * for update
     * @param xid Transaction ID
     * @param update
     * @return The number of fields modified
     * @throws Exception
     */
    public int update(long xid, Update update) throws Exception {
        List<Long> uids = parseWhere2(xid,update.where);
        int num=0;


         //Read the data according to the UID and modify the data information according to the update object
        for(Long uid:uids){
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid,false);
            if(raw==null)continue;
            Map<String, Type> entry = parseEntry(raw);


            List<Field> indexList=new ArrayList<>();
            List<Type> typeList=new ArrayList<>();
            for (int i=0;i<update.fieldName.length;i++) {
                Field field = fieldBaseMessage.get(update.fieldName[i]);
                Type type = TypeFactory.unParseString(field.fieldType, update.value[i]);
                if(field.isIndex()){
                    if(((TableManagerImpl)tbm).vm.delete(xid, uid)){
                        indexList.add(field);
                        typeList.add(type);
                    }
                }else {
                    entry.put(update.fieldName[i],type);
                }
            }


            //Turn MAP into RAW
            byte[] bytes = entry2Raw(entry);

            /*  There are two cases, whether there is an index,
                if there is an index, delete the original data, and add new data
                If there is no index, use the undo form to add the back link
            */
            if(indexList.size()==0){
                ((TableManagerImpl)tbm).vm.update(xid, uid,bytes);
            }else{
                long newDataUid = ((TableManagerImpl)tbm).vm.insert(xid, bytes);
                for (int i=0;i<indexList.size();i++) {

                    indexList.get(i).bt.insert(typeList.get(i),newDataUid);
                }
            }

            num++;

        }

        return num;
    }

    /**
     * It is used to convert where into an area object for resolution
     */
    private static class Area{
        String fieldName;
        Type left;
        Type right;

        public Area(String filedName,Type left, Type right) {
            this.fieldName=filedName;
            this.left = left;
            this.right = right;
        }
    }

    /**
     * fixme: There are no optimization query criteria
     * @param where
     * @return The UID retrieved according to the WHERE condition
     * @throws Exception
     */
    private List<Long> parseWhere2(long xid,Where2 where) throws Exception{

        LinkedList<List<Long>> uidList=new LinkedList<>();


        List<Object> singleExpressions = where.singleExpression;
        List<String> logicOp = where.logicOp;
        int pos=0;
        while(pos<singleExpressions.size()){
            Object o = singleExpressions.get(pos);
            if(o instanceof SingleExpression){
                SingleExpression se = (SingleExpression) o;
                if(fieldBaseMessage.containsKey(se.field)){
                    Area area = new Area(se.field, null, null);
                    Field field1 = fieldBaseMessage.get(se.field);
                    parseSingleExpression(field1.fieldType ,area,se);
                    List<Long> search=null;
                    if(field1.bt!=null){
                         search= field1.search(area.left, area.right);
                    }else{
                        search=scanAll(xid,field1,area);
                    }
                    uidList.add(search);
                }else{
                    throw Error.FieldNotFoundException;
                }


            } else if (o instanceof Where2) {
                List<Long> list = parseWhere2(xid,(Where2) o);
                uidList.add(list);

            }

            pos++;

        }

        for (int i=0,j=0;i<logicOp.size();i++,j++) {
            String s = logicOp.get(i);
            if(s.equals("and")){
                //1.把两个list进行or运算，讲结果存在上一个list中
                uidList.add(j,computeAnd(uidList.get(j),uidList.get(j+1)));
                //2.删除第一个list
                uidList.remove(j+1);
                uidList.remove(j+1);
                j--;
            }
        }

        while(uidList.size()>1){
            uidList.addFirst(computeOr(uidList.get(0),uidList.get(1)));
            uidList.remove(1);
            uidList.remove(1);
        }





    return uidList.get(0);

    }

    /**
     * Simple full-disk scan, not optimized
     * @param xid
     * @param field
     * @param area
     * @return
     * @throws Exception
     */
    private List<Long> scanAll(long xid,Field field,Area area) throws Exception {
        List<Long> uids=new ArrayList<>();
        List<Long> ans=new ArrayList<>();
        //Full scan to get all UIDs
        for(Field field1:fields){
            if(field1.isIndex()){
                uids.addAll(field1.bt.searchRange(null,null));
                break;
            }
        }
        //Judge the conditions
        for (Long uid : uids) {
            DataItem dataItem = dm.read(uid);
            byte[] data = ((TableManagerImpl) tbm).vm.read(xid, uid,true);
            if(data == null) continue;
            Map<String, Type> entry = parseEntry(data);
            Type type = entry.get(area.fieldName);
            if((area.left==null || type.compareTo(area.left)>0) &&(area.right==null || type.compareTo(area.right)<0)){
                ans.add(uid);
            }
        }
        return ans;
    }

    /**
     *  Calculate the intersection of two UIDs
     * @param uid1
     * @param uid2
     * @return
     */
    private List<Long> computeAnd(List<Long> uid1, List<Long> uid2) {

        return  uid1.stream().filter(uid2::contains).collect(Collectors.toList());


    }

    /**
     * Calculate the union of two UIDs
     * @param uid1
     * @param uid2
     * @return
     */

    private List<Long> computeOr(List<Long> uid1, List<Long> uid2) {
        int length=uid1.size();

        uid1.addAll(uid2);

        List<Long> collect = uid1.stream().distinct().collect(Collectors.toList());

        return collect;

    }

    /**
     * Convert the SingleExpression into an Area object for subsequent lookups
     * @param fieldType
     * @param area
     * @param singleExp
     */
    private void parseSingleExpression(String fieldType,Area area,SingleExpression singleExp){
        Type type = TypeFactory.unParseString(fieldType, singleExp.value);
        if(">".equals(singleExp.compareOp)&&(area.left==null||area.left.compareTo(type)<0)){
            area.left=type;
        }else if("<".equals(singleExp.compareOp)&&(area.left==null||area.left.compareTo(type)>0)){
            area.right= type;
        }else{
            area.left= type;
            area.right= type;
        }
    }


    /**
     * Convert entry to string
     * @param entry
     * @return The string used for printing
     */
    private String printEntry(Map<String, Type> entry) {
        StringBuilder sb = new StringBuilder("[");
//        for (int i = 0; i < fields.size(); i++) {
//            Field field = fields.get(i);
//            if(entry.get(field.fieldName)!=null){
//                sb.append(field.printValue(entry.get(field.fieldName)));
//            }
//
//
//            if(i == fields.size()-1) {
//                sb.append("]");
//            } else {
//                sb.append(", ");
//            }
//        }
        //The above commented code is optimized using Steam streams
        fields.forEach(a->{
            if(entry.containsKey(a.fieldName)){
                sb.append(a.printValue(entry.get(a.fieldName)));
                sb.append(", ");
            }
        });
        sb.delete(sb.length()-2,sb.length());
        sb.append("]");
        return sb.toString();
    }


    /**
     * Convert the RAW read by the UID to MAP
     * @param raw
     * @return
     */
    private Map<String,Type> parseEntry(byte[] raw,String[] fields1) {
        int pos = 0;
        Map<String, Type> entry = new HashMap<>();

        for(Field field:fields){

            Type entryType = TypeFactory.unParseRaw(field.fieldType, Arrays.copyOfRange(raw, pos, raw.length));
            if(fields1.length==0||Arrays.stream(fields1).anyMatch(a->a.equals(field.fieldName))){
                entry.put(field.fieldName,entryType);
            }
            pos+=entryType.getSize();
        }
        return entry;
    }

    private Map<String,Type> parseEntry(byte[] raw) {
      return parseEntry(raw,new String[0]);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for(Field field : fields) {
            sb.append(field.toString());
            if(field == fields.get(fields.size()-1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    public List<Field> getFields() {
        return fields;
    }

}
