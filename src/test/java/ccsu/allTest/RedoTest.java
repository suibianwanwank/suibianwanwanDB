package ccsu.allTest;

import com.ccsu.log.Recover;
import com.ccsu.log.RedoLogManager;
import com.ccsu.page.PageImpl;
import org.junit.Test;

public class RedoTest {
    final String path="D://db//mydata";
    @Test
    public void test(){

        RedoLogManager redoLogManager=RedoLogManager.create(path);
        byte[] log1 = RedoLogManager.insertLog(123, new PageImpl(5,new byte[0],null), "dsadad".getBytes());
        byte[] log2 = RedoLogManager.insertLog(123, new PageImpl(5,new byte[0],null), "4546".getBytes());
        redoLogManager.log(log1);
        redoLogManager.log(log2);
    }
    @Test
    public void test2() throws Exception {
        //
        RedoLogManager redoLogManager=RedoLogManager.open(path);
        Recover.recover(null,redoLogManager,null);
    }
}
