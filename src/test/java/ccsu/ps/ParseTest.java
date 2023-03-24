package ccsu.ps;

import com.ccsu.tp.Type;
import com.ccsu.tp.TypeFactory;
import com.ccsu.utils.Bytes;
import org.junit.Test;

public class ParseTest {
    @Test
    public void testParse(){
        Type type = TypeFactory.unParseString("string", "sdada");
        int size = type.getSize();
        byte[] bytes = type.parseRaw();
        Type type1 = TypeFactory.unParseRaw("string",Bytes.concat(bytes, "sda".getBytes()));

        System.out.println(type.parseString());
        System.out.println(size);
        System.out.println(type1.getData());
        System.out.println(type1.getSize());
    }

    @Test
    public void test2(){

        String a=null;
        String b="xx";
        System.out.println(b.equals(a));


    }
}
