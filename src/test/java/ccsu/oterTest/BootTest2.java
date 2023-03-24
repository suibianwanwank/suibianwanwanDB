package ccsu.oterTest;

import com.ccsu.dm.DataManager;
import com.ccsu.dm.DataManagerImpl;
import com.ccsu.log.RedoLogManager;
import com.ccsu.log.UndoLogManager;
import com.ccsu.statement.*;
import com.ccsu.tb.TableManager;
import com.ccsu.tb.TableManagerImpl;
import com.ccsu.tm.TransactionManager;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.vm.VersionManager;
import com.ccsu.vm.VersionManagerImpl;
import org.junit.Test;

public class BootTest2 {

    public static final long DEFAULT_MEM = (1<<20)*64;
    @Test
    public void testBootCreat() throws Exception {

        String path="D://db//mydata";
        RedoLogManager redoLogManager = RedoLogManager.create(path);
        UndoLogManager undoLogManager = UndoLogManager.create(path);
        DataManager dm = DataManagerImpl.create(path, DEFAULT_MEM,redoLogManager);
        TransactionManager tm = TransactionManagerImpl.create(path);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        TableManager tbm = TableManagerImpl.create(vm,path, dm);


        Create create=new Create();
        create.fieldName=new String[]{"uid","age"};
        create.fieldType=new String[]{"int","long"};
        create.index=new String[]{"uid"};
        create.tableName="myFirstTable";

        Create create2=new Create();
        create2.fieldName=new String[]{"uid2","age2"};
        create2.fieldType=new String[]{"int","long"};
        create2.index=new String[]{"uid2"};
        create2.tableName="myFirstTable2";






        tbm.create(0,create);
        tbm.create(0,create2);

        byte[] show = tbm.show(TransactionManagerImpl.SUPER_XID);
        System.out.println(new String(show));
//        byte[] bytes2 = tm.create(0, create2);




    }

    @Test
    public void testBootOpen(){
//        TypeFactory.initTypeFactor();
//        String path="D://db//mydata";
//        DataManager dm = DataManagerImpl.open(path, DEFAULT_MEM);

////        TableManager tm = TableManagerImpl.open(path, dm);
//        String s=new String(tm.show(0));
//        System.out.println(s);
    }

}
