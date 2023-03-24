package ccsu.parse;

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

public class RollBackTest {
    public static final long DEFAULT_MEM = (1<<20)*64;

    private TableManager init() throws Exception {
        String path="D://db//mydata";
        UndoLogManager undoLogManager = UndoLogManager.create(path);
        RedoLogManager redoLogManager = RedoLogManager.create(path);
        DataManager dm = DataManagerImpl.create(path, DEFAULT_MEM,redoLogManager);
        TransactionManager tm = TransactionManagerImpl.create(path);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        TableManager tbm = TableManagerImpl.create(vm,path, dm);
        return tbm;
    }
    @Test
    public void test1() throws Exception {
        String path="D://db//mydata";
        UndoLogManager undoLogManager = UndoLogManager.create(path);
        RedoLogManager redoLogManager = RedoLogManager.create(path);

        DataManager dm = DataManagerImpl.create(path, DEFAULT_MEM,redoLogManager);
        TransactionManager tm = TransactionManagerImpl.create(path);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        TableManager tbm = TableManagerImpl.create(vm,path, dm);
        String test="create table table1(\n" +
                "\ta22 int ,\n" +
                "\tb22 long ,\n" +
                "index(a22) \n" +
                ");";

        Executor executor=new Executor(0,tbm);
        byte[] execute = executor.execute(test.getBytes());


        String test1="insert into table1(a22,b22) values(3,4);";
        String test2="insert into table1(a22,b22) values(45,78);";
        String test3="insert into table1(a22,b22) values(6,7);";
        byte[] execute1 = executor.execute(test1.getBytes());
        byte[] execute2 = executor.execute(test2.getBytes());
        byte[] execute3 = executor.execute(test3.getBytes());

        String test4="begin;";
//        String test5="insert into table1(a22,b22) values(12,13);";
        String test5="update table1 set b22=7 where a22=45;";
        String test6="commit;";
        String test61="rollback;";

        Executor executor1=new Executor(0,tbm);
        executor1.execute(test4.getBytes());
        executor1.execute(test5.getBytes());
        executor1.execute(test61.getBytes());



        String test7="select a22,b22 from table1 where a22 > 5;";
        byte[] execute6 = executor.execute(test7.getBytes());
        System.out.println(new String(execute6));







    }



    @Test
    public void test2() throws Exception {
        String path="D://db//mydata";
        UndoLogManager undoLogManager = UndoLogManager.create(path);
        RedoLogManager redoLogManager = RedoLogManager.create(path);

        DataManager dm = DataManagerImpl.create(path, DEFAULT_MEM,redoLogManager);
        TransactionManager tm = TransactionManagerImpl.create(path);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        TableManager tbm = TableManagerImpl.create(vm,path, dm);
        String test="create table table1(\n" +
                "\ta22 int ,\n" +
                "\tb22 long ,\n" +
                "index(a22) \n" +
                ");";

        Executor executor=new Executor(0,tbm);
        byte[] execute = executor.execute(test.getBytes());


        String test1="insert into table1(a22,b22) values(3,4);";
        String test2="insert into table1(a22,b22) values(45,78);";
        String test3="insert into table1(a22,b22) values(6,7);";
        byte[] execute1 = executor.execute(test1.getBytes());
        byte[] execute2 = executor.execute(test2.getBytes());
        byte[] execute3 = executor.execute(test3.getBytes());

        String test4="begin;";
//        String test5="insert into table1(a22,b22) values(12,13);";
        String test5="update table1 set b22=7 where a22=45;";
        String test6="commit;";
        String test61="rollback;";

        Executor executor1=new Executor(0,tbm);
        executor1.execute(test4.getBytes());
        executor1.execute(test5.getBytes());




        String test7="select a22,b22 from table1 where a22 > 5;";
        byte[] execute6 = executor.execute(test7.getBytes());
        System.out.println(new String(execute6));
        executor1.execute(test6.getBytes());


        byte[] execute8 = executor.execute(test7.getBytes());
        System.out.println(new String(execute8));





    }


    @Test
    public void test3() throws Exception {
        String path="D://db//mydata";
        RedoLogManager redoLogManager=RedoLogManager.create(path);
        UndoLogManager undoLogManager = UndoLogManager.create(path);
        DataManager dm = DataManagerImpl.create(path, DEFAULT_MEM,redoLogManager);
        TransactionManager tm = TransactionManagerImpl.create(path);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        TableManager tbm = TableManagerImpl.create(vm,path, dm);
        String test="create table table1(\n" +
                "\ta22 int ,\n" +
                "\tb22 long ,\n" +
                "index(a22) \n" +
                ");";

        Executor executor=new Executor(0,tbm);
        byte[] execute = executor.execute(test.getBytes());

        String test4="begin;";
        String test1="insert into table1(a22,b22) values(3,4);";
        String test2="insert into table1(a22,b22) values(45,78);";
        String test3="insert into table1(a22,b22) values(6,7);";

        String test7="select a22,b22 from table1 where a22 > 5;";
        byte[] execute1 = executor.execute(test1.getBytes());
        byte[] execute2 = executor.execute(test2.getBytes());
        byte[] execute3 = executor.execute(test3.getBytes());
        executor.execute(test4.getBytes());
        byte[] execute5 = executor.execute(test7.getBytes());
        System.out.println(new String(execute5));



//        String test5="insert into table1(a22,b22) values(12,13);";
        String test5="update table1 set b22=7 where a22=45;";
        String test51="update table1 set a22=14 where a22=45;";
        String test6="commit;";
        String test61="rollback;";

        Executor executor1=new Executor(0,tbm);
        executor1.execute(test4.getBytes());
        executor1.execute(test5.getBytes());
        executor1.execute(test51.getBytes());

        byte[] execute51 = executor.execute(test7.getBytes());
        System.out.println(new String(execute51));

        executor1.execute(test61.getBytes());







        byte[] execute4 = executor.execute(test7.getBytes());
        System.out.println(new String(execute4));

        byte[] execute7 = executor.execute(test6.getBytes());







    }



    @Test
    public void test4() throws Exception {
        String path="D://db//mydata";
        UndoLogManager undoLogManager = UndoLogManager.create(path);
        RedoLogManager redoLogManager = RedoLogManager.create(path);
        DataManager dm = DataManagerImpl.create(path, DEFAULT_MEM,redoLogManager);
        TransactionManager tm = TransactionManagerImpl.create(path);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        TableManager tbm = TableManagerImpl.create(vm,path, dm);
        String test="create table table1(\n" +
                "\ta22 int ,\n" +
                "\tb22 long ,\n" +
                "index(a22) \n" +
                ");";

        Executor executor=new Executor(0,tbm);
        byte[] execute = executor.execute(test.getBytes());

        String test4="begin;";
        String test1="insert into table1(a22,b22) values(3,4);";
        String test2="insert into table1(a22,b22) values(45,78);";
        String test3="insert into table1(a22,b22) values(6,7);";

        String test7="select a22,b22 from table1 where a22 > 5;";
        byte[] execute1 = executor.execute(test1.getBytes());
        byte[] execute2 = executor.execute(test2.getBytes());
        byte[] execute3 = executor.execute(test3.getBytes());

        executor.execute(test4.getBytes());

        byte[] execute5 = executor.execute(test7.getBytes());
        System.out.println(new String(execute5));



//        String test5="insert into table1(a22,b22) values(12,13);";
        String test5="insert into table1(a22,b22) values(405,708);";
        String test6="commit;";
        String test61="rollback;";

        Executor executor1=new Executor(0,tbm);
        //begin
        executor1.execute(test4.getBytes());
        //插入
        executor1.execute(test5.getBytes());
        //查找
        byte[] execute51 = executor.execute(test7.getBytes());
        System.out.println(new String(execute51));
        //提交 or 回滚
//        executor1.execute(test6.getBytes());
        executor1.execute(test61.getBytes());



        //查找
        byte[] execute4 = executor.execute(test7.getBytes());
        System.out.println(new String(execute4));

        //提交
        byte[] execute7 = executor.execute(test6.getBytes());


        //查找
        byte[] execute42 = executor.execute(test7.getBytes());
        System.out.println(new String(execute42));


        dm.close();



    }
}
