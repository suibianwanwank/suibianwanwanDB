package ccsu;

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

public class BenchMarkResource2 {

    public static final long DEFAULT_MEM = (1<<18)*64;

    public static TableManager tableManager;

    static {
        try {
            tableManager = open();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static TableManager open() throws Exception {
        String path="D://db//mydata";
        UndoLogManager undoLogManager = UndoLogManager.open(path);
        RedoLogManager redoLogManager = RedoLogManager.open(path);

        TransactionManager tm = TransactionManagerImpl.open(path);
        DataManager dm = DataManagerImpl.open(path,tm, DEFAULT_MEM,redoLogManager);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        TableManager tbm = TableManagerImpl.open(vm,path, dm);
        return tbm;
    }


    private static void init2(TableManager tableManager) throws Exception {
        String test="create table table1(\n" +
                "\ta22 int ,\n" +
                "\tb22 long ,\n" +
                "index(a22) \n" +
                ");";
        Executor executor=new Executor(0,tableManager);
        byte[] execute = executor.execute(test.getBytes());
        String test1="insert into table1(a22,b22) values(3,4);";
        String test2="insert into table1(a22,b22) values(45,78);";
        String test3="insert into table1(a22,b22) values(6,7);";
        byte[] execute1 = executor.execute(test1.getBytes());
        byte[] execute2 = executor.execute(test2.getBytes());
        byte[] execute3 = executor.execute(test3.getBytes());

    }
}
