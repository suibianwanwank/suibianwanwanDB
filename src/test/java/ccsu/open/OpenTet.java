//package ccsu.open;
//
//import com.ccsu.dm.DataManager;
//import com.ccsu.dm.DataManagerImpl;
//import com.ccsu.log.UndoLogManager;
//import com.ccsu.server.Executor;
//import com.ccsu.tb.TableManager;
//import com.ccsu.tb.TableManagerImpl;
//import com.ccsu.tm.TransactionManager;
//import com.ccsu.tm.TransactionManagerImpl;
//import com.ccsu.vm.VersionManager;
//import com.ccsu.vm.VersionManagerImpl;
//import org.junit.Test;
//
//public class OpenTet {
//
//    public static final long DEFAULT_MEM = (1<<20)*64;
//    @Test
//    public void test01() throws Exception {
//        String path="D://db//mydata";
//        UndoLogManager undoLogManager = UndoLogManager.open(path);
//        DataManager dm = DataManagerImpl.open(path, DEFAULT_MEM);
//        TransactionManager tm = TransactionManagerImpl.open(path);
//        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
//        TableManager tbm = TableManagerImpl.open(vm,path, dm);
//
//
//        Executor executor=new Executor(0,tbm);
//        String test7="select a22,b22 from table1 where a22 > 5;";
//        byte[] execute = executor.execute(test7.getBytes());
//        System.out.println(new String(execute));
//    }
//
//
//}
