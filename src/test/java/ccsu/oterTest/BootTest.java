//package ccsu;
//
//import com.ccsu.dm.DataManager;
//import com.ccsu.dm.DataManagerImpl;
//import com.ccsu.log.UndoLogManager;
//import com.ccsu.statement.*;
//import com.ccsu.tb.TableManager;
//import com.ccsu.tb.TableManagerImpl;
//import com.ccsu.tm.TransactionManager;
//import com.ccsu.tm.TransactionManagerImpl;
//import com.ccsu.vm.VersionManager;
//import com.ccsu.vm.VersionManagerImpl;
//import org.junit.Test;
//
//public class BootTest {
//
//    public static final long DEFAULT_MEM = (1<<20)*64;
//    @Test
//    public void testBootCreat() throws Exception {
//
//        String path="D://db//mydata";
//        UndoLogManager undoLogManager = UndoLogManager.create(path);
//        DataManager dm = DataManagerImpl.create(path, DEFAULT_MEM);
//        TransactionManager tm = TransactionManagerImpl.create(path);
//         VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
//        TableManager tbm = TableManagerImpl.create(vm,path, dm);
//
//
//        Create create=new Create();
//        create.fieldName=new String[]{"uid","age"};
//        create.fieldType=new String[]{"int","long"};
//        create.index=new String[]{"uid"};
//        create.tableName="myFirstTable";
//
//        Create create2=new Create();
//        create2.fieldName=new String[]{"uid2","age2"};
//        create2.fieldType=new String[]{"int","long"};
//        create2.index=new String[]{"uid2"};
//        create2.tableName="myFirstTable2";
//
//
//
//
//
//        byte[] bytes = tbm.create(0, create);
////        byte[] bytes2 = tm.create(0, create2);
//
//
//        Insert insert=new Insert();
//        insert.tableName="myFirstTable";
//        insert.values=new String[]{"4","5"};
//        byte[] x = tbm.insert(4, insert);
//
//        Insert insert2=new Insert();
//        insert2.tableName="myFirstTable";
//        insert2.values=new String[]{"5","6"};
//
//        byte[] x2 = tbm.insert(5, insert2);
//
//
//
//        Insert insert3=new Insert();
//        insert3.tableName="myFirstTable";
//        insert3.values=new String[]{"7","8"};
//
//        byte[] x3 = tbm.insert(6, insert3);
////
////        Insert insert4=new Insert();
////        insert3.tableName="myFirstTable";
////        insert3.values=new String[]{"9","10"};
////
////        byte[] x4 = tm.insert(6, insert4);
//
//        Insert insert5=new Insert();
//        insert5.tableName="myFirstTable";
//        insert5.values=new String[]{"6","100"};
//
//        byte[] x5 = tbm.insert(6, insert5);
//
//        Insert insert6=new Insert();
//        insert6.tableName="myFirstTable";
//        insert6.values=new String[]{"5","20"};
//        byte[] x6 = tbm.insert(6, insert6);
//
//        Insert insert7=new Insert();
//        insert7.tableName="myFirstTable";
//        insert7.values=new String[]{"6","12"};
//
//        byte[] x7 = tbm.insert(6, insert7);
//
//
//        Insert insert8=new Insert();
//        insert8.tableName="myFirstTable";
//        insert8.values=new String[]{"8","12"};
//
//        byte[] x8 = tbm.insert(6, insert8);
//
//
//        Select select=new Select();
//        select.tableName="myFirstTable";
//
//
//        byte[] read = tbm.read(1, select);
//        System.out.println(new String(read));
////        System.out.println(new String(insert1));
//        String s=new String(tbm.show(0));
//        System.out.println(s);
//    }
//
//    @Test
//    public void testBootOpen(){
////        TypeFactory.initTypeFactor();
//        String path="D://db//mydata";
//        DataManager dm = DataManagerImpl.open(path, DEFAULT_MEM);
//
//////        TableManager tm = TableManagerImpl.open(path, dm);
////        String s=new String(tm.show(0));
////        System.out.println(s);
//    }
//
//}
