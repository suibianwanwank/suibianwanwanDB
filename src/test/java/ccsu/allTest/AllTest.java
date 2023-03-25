package ccsu.allTest;

import com.ccsu.dm.DataManager;
import com.ccsu.dm.DataManagerImpl;
import com.ccsu.log.RedoLogManager;
import com.ccsu.log.UndoLogManager;
import com.ccsu.server.Executor;
import com.ccsu.tb.TableManager;
import com.ccsu.tb.TableManagerImpl;
import com.ccsu.tm.TransactionManager;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.vm.LockManager;
import com.ccsu.vm.VersionManager;
import com.ccsu.vm.VersionManagerImpl;
import org.junit.Test;

public class AllTest {
    public static final long DEFAULT_MEM = (1<<20)*64;
    private TableManager create() throws Exception {
        String path="D://db//mydata";
        RedoLogManager redoLogManager=RedoLogManager.create(path);
        UndoLogManager undoLogManager = UndoLogManager.create(path);
        DataManager dm = DataManagerImpl.create(path, DEFAULT_MEM,redoLogManager);
        TransactionManager tm = TransactionManagerImpl.create(path);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        TableManager tbm = TableManagerImpl.create(vm,path, dm);
        return tbm;
    }

    private TableManager open() throws Exception {
        String path="D://db//mydata";
        UndoLogManager undoLogManager = UndoLogManager.open(path);
        RedoLogManager redoLogManager = RedoLogManager.open(path);

        TransactionManager tm = TransactionManagerImpl.open(path);
        DataManager dm = DataManagerImpl.open(path,tm, DEFAULT_MEM,redoLogManager);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        TableManager tbm = TableManagerImpl.open(vm,path, dm);
        return tbm;
    }

    private void init2(TableManager tableManager) throws Exception {
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

    /**
     * 1.Test project startup and table building
     * 2.Test basic insert and lookups
     * 3.Test transaction commit three isolation levels
     * @throws Exception
     */
    @Test
    public void test01() throws Exception {
        TableManager tableManager = create();
        init2(tableManager);

        byte[] begin = "begin repeatable read;".getBytes();
        byte[] update1 = "update table1 set b22=7 where a22=45;".getBytes();
        byte[] insert1 = "insert into table1(a22,b22) values(150,48);".getBytes();
        byte[] commit = "commit;".getBytes();
        byte[] rollback = "rollback;".getBytes();

        byte[] select = "select a22,b22 from table1 where a22 > 5;".getBytes();

        //事务
        Executor transaction1=new Executor(0,tableManager);
        Executor transaction2=new Executor(0,tableManager);
        transaction1.execute(begin);
        transaction2.execute(begin);

        transaction2.execute(insert1);
        //读未提交应该可以读到数据
        byte[] res = transaction1.execute(select);
        System.out.println(new String(res));

        transaction2.execute(commit);

        byte[] res2 = transaction1.execute(select);
        System.out.println(new String(res2));

        transaction1.execute(commit);
//        dm.close();
    }


    /**
     * Test the redo log recovery startu
     * @throws Exception
     */

    @Test
    public void test2() throws Exception {
        TableManager tableManager = open();
        byte[] select = "select a22,b22 from table1 where a22 > 5;".getBytes();
        Executor transaction1=new Executor(0,tableManager);
        byte[] execute = transaction1.execute(select);
        System.out.println(new String(execute));
    }

    /**
     * Test large volumes of data insertions and lookups
     * @throws Exception
     */
    @Test
    public void test03() throws Exception {
        TableManager tableManager = open();
        Executor transaction=new Executor(0,tableManager);
        for(int i=120;i<1000;i++){
            String insert="insert into table1(a22,b22) values("+i+","+(i+1)+");";
            transaction.execute(insert.getBytes());
        }
        byte[] select = "select a22 from table1 where a22 > 5;".getBytes();
        Executor transaction1=new Executor(0,tableManager);
        byte[] execute = transaction1.execute(select);
        System.out.println(new String(execute));
    }

    /**
     * Test the creation of multiple tables and multiple indexes on a single table
     * @throws Exception
     */
    @Test
    public void test04() throws Exception {
        TableManager tableManager = open();

        String create="create table table2(\n" +
                "\tp11 int ,\n" +
                "\tq22 long ,\n" +
                "index(p11) , \n" +
                "index(q22) \n" +
                ");";
        Executor transaction=new Executor(0,tableManager);
//        transaction.execute(create.getBytes());
//        byte[] insert1 = "insert into table2(p11,q22) values(111,222);".getBytes();
//        transaction.execute(insert1);

        byte[] select1 = "select * from table1 where b22 > 200;".getBytes();
        byte[] select2 = "select * from table2 where p11 > 5;".getBytes();
        byte[] select3 = "select * from table2 where q22 > 5;".getBytes();
        Executor transaction1=new Executor(0,tableManager);
        byte[] execute = transaction1.execute(select1);
        System.out.println(new String(execute));
        System.out.println("===========================");
        Executor transaction2=new Executor(0,tableManager);
        byte[] execute2 = transaction1.execute(select2);
        System.out.println(new String(execute2));
        byte[] execute3 = transaction1.execute(select3);
        System.out.println(new String(execute3));
    }

    @Test
    public void test05() throws Exception {
        TableManager tableManager = open();

        byte[] begin = "begin repeatable read;".getBytes();
        byte[] update1 = "update table1 set b22=7 where a22=45;".getBytes();
        byte[] update3 = "update table1 set b22=100 where a22=3;".getBytes();

        byte[] update2 = "update table1 set b22=60 where a22=3;".getBytes();
        byte[] update4 = "update table1 set b22=999 where a22=45;".getBytes();
        byte[] commit = "commit;".getBytes();
        byte[] rollback = "rollback;".getBytes();
        Executor executor=new Executor(0,tableManager);
        Executor executor1=new Executor(0,tableManager);

        Thread thread1 = new Thread(() -> {
            try {
                executor.execute(begin);
                executor.execute(update1);
                Thread.sleep(1000);
                executor.execute(update3);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        });
        thread1.start();

        Thread thread = new Thread(() -> {
            try {
                executor1.execute(begin);
                executor1.execute(update2);
                Thread.sleep(1000);
                executor1.execute(update4);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        });
        thread.start();

        thread.join();
        thread1.join();
        executor.execute(commit);
        byte[] select = "select a22,b22 from table1 where a22 > 1;".getBytes();
        byte[] execute = executor1.execute(select);
        System.out.println(new String(execute));

    }
    @Test
    public void test6() throws Exception {
        TableManager tableManager = open();

        byte[] select = "select a22,b22 from table1 where a22 > 1;".getBytes();
        Executor executor=new Executor(0,tableManager);
        executor.execute(select);




    }




}
