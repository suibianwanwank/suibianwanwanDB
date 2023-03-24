package ccsu.dm;

import com.ccsu.dm.DataManager;
import com.ccsu.dm.DataManagerImpl;
import com.ccsu.log.RedoLogManager;
import com.ccsu.log.UndoLogManager;
import com.ccsu.server.Executor;
import com.ccsu.tb.TableManager;
import com.ccsu.tb.TableManagerImpl;
import com.ccsu.tm.TransactionManager;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.vm.VersionManager;
import com.ccsu.vm.VersionManagerImpl;
import org.junit.Test;

public class TestDataItem {
    public static final long DEFAULT_MEM = (1<<20)*64;

    @Test
    public void Test() throws Exception {
        String path="D://db//mydata";
        RedoLogManager redoLogManager = RedoLogManager.create(path);
        UndoLogManager undoLogManager = UndoLogManager.create(path);
        DataManager dm = DataManagerImpl.create(path, DEFAULT_MEM,redoLogManager);
        TransactionManager tm = TransactionManagerImpl.create(path);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        TableManager tbm = TableManagerImpl.create(vm,path, dm);
        String test="create table table1(\n" +
                "\ta22 int index,\n" +
                "\tb22 long\n" +
                ")";
        String test1="insert into table1(a22,b22) values(3,4);";
        String test2="insert into table1(a22,b22) values(45,78);";
        String test3="insert into table1(a22,b22) values(6,7);";
        Executor executor=new Executor(0,tbm);
        byte[] execute = executor.execute(test.getBytes());

        System.out.println(new String(execute));
        byte[] execute1 = executor.execute(test1.getBytes());
        System.out.println(new String(execute1));

        byte[] execute2 = executor.execute(test2.getBytes());
        System.out.println(new String(execute2));

        byte[] execute3 = executor.execute(test3.getBytes());
        System.out.println(new String(execute3));

        String test4="select a22,b22 from table1 where  a22<80;";
        byte[] execute4 = executor.execute(test4.getBytes());
        System.out.println(new String(execute4));

//        dm.read()

    }
}
