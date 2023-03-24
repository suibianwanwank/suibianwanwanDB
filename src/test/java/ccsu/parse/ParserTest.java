package ccsu.parse;

import com.ccsu.dm.DataManager;
import com.ccsu.dm.DataManagerImpl;
import com.ccsu.log.RedoLogManager;
import com.ccsu.log.UndoLogManager;
import com.ccsu.server.Executor;
import com.ccsu.server.Parser;
import com.ccsu.server.Tokenizer;
import com.ccsu.statement.*;
import com.ccsu.tb.TableManager;
import com.ccsu.tb.TableManagerImpl;
import com.ccsu.tm.TransactionManager;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.vm.VersionManager;
import com.ccsu.vm.VersionManagerImpl;
import org.junit.Test;

public class ParserTest {

    public static final long DEFAULT_MEM = (1<<20)*64;
    @Test
    public void testCreatParse() throws Exception {
        String test="create table table1(\n" +
                "\ta int index,\n" +
                "\tb long\n" +
                ")";
        byte[] statement = test.getBytes();
        Create parse = (Create) Parser.parse(statement);
        System.out.println(parse);

    }


    @Test
    public void testInsertParse() throws Exception {
        String test="insert into table1(a2,b2,c2) values(3,tt,4);";
        byte[] statement = test.getBytes();
        Insert parse = (Insert) Parser.parse(statement);
        System.out.println(parse);

    }


    @Test
    public void testCreate() throws Exception {
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

//        String test4="select a22,b22 from table1 where  a22<80;";
//        byte[] execute4 = executor.execute(test4.getBytes());
//        System.out.println(new String(execute4));
//
        String test5="delete from table1 where a22=45;";
        byte[] execute5 = executor.execute(test5.getBytes());
        System.out.println(new String(execute5));

        String test6="select a22,b22 from table1 where  ((a22<80 or a22<12) and a22>13) and a22>5;";
        byte[] execute6 = executor.execute(test6.getBytes());
        System.out.println(new String(execute6));

    }

    @Test
    public void TestSelect() throws Exception {
        String test="select id,name from table1 where id>10 and name<30;";
        Select parse = (Select) Parser.parse(test.getBytes());
        System.out.println(parse);
    }
    @Test
    public void TestDelete() throws Exception {
        String test="delete from able where apex=20 and lol<10;";
        Delete parse = (Delete)Parser.parse(test.getBytes());
        System.out.println(parse);
    }


    @Test
    public void TestWhere() throws Exception {
        String test="((at1>10 or at2<5) or at2>7) and at1<5;";
        Tokenizer tokenizer=new Tokenizer(test.getBytes());
        Where2 parse = Parser.parseWhere2(tokenizer);
        System.out.println(parse);
    }

    @Test
    public void TestCreate() throws Exception {
        String test="table table1(\n" +
                "\ta22 int ,\n" +
                "\tb22 long,\n" +
                "index(a22));";
        Tokenizer tokenizer=new Tokenizer(test.getBytes());
        Create parse = Parser.parseCreate(tokenizer);
        System.out.println(parse);
    }
    @Test
    public void testSubstantialDataInsert() throws Exception {
        String path="D://db//mydata";
        RedoLogManager redoLogManager = RedoLogManager.create(path);
        UndoLogManager undoLogManager = UndoLogManager.create(path);
        DataManager dm = DataManagerImpl.create(path, DEFAULT_MEM,redoLogManager);
        TransactionManager tm = TransactionManagerImpl.create(path);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        TableManager tbm = TableManagerImpl.create(vm,path, dm);

        String test1="create table table1(a22 int,b22 long,index(a22));";

        Executor executor=new Executor(0,tbm);
        executor.execute(test1.getBytes());
//        for(int i=1;i<120;i++){
//            if(i==4){
//                System.out.println("t");
//            }
//            System.out.println(i+":");
//            String test2="insert into table1(a22,b22) values("+i+","+i+");";
//            byte[] execute = executor.execute(test2.getBytes());
//            System.out.println(new String(execute));
//        }
        String x="begin";
        String x2="rollback";
        String test0="insert into table1(a22,b22) values (150,170) ;";
        byte[] execute32 = executor.execute(x.getBytes());
        byte[] execute33 = executor.execute(test0.getBytes());

        byte[] execute34 = executor.execute(x2.getBytes());

        String test6="select a22,b22 from table1 where a22>"+120+" ;";
        byte[] execute6 = executor.execute(test6.getBytes());
        System.out.println(new String(execute6));

        String testx1="insert into table1(a22,b22) values (100,170) ;";
        String testx="select a22,b22 from table1 where a22>"+100+" ;";
        byte[] executex1 = executor.execute(testx1.getBytes());

        for(int i=1;i<120;i++){

            System.out.println(i+":");
            String test2="insert into table1(a22,b22) values("+i+","+i+");";
            byte[] execute = executor.execute(test2.getBytes());
            System.out.println(new String(execute));
        }

        byte[] executex2 = executor.execute(testx.getBytes());
        System.out.println(new String(executex2));
    for(int i=3;i<7;i++){
        String test7="select a22,b22 from table1 where a22>"+i*10+" ;";
        byte[] execute7 = executor.execute(test7.getBytes());
        System.out.println("=================");
        System.out.println(new String(execute7));
        System.out.println("=================");
    }



    }




}
