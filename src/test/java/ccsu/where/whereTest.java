package ccsu.where;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class whereTest {
    @Test
    public void test(){
        List<Object> objectList=new ArrayList<>();

        objectList.add(new String("t"));
        objectList.add(123);

        for (Object o : objectList) {
            if(o instanceof Integer){
                System.out.println("integer:"+(Integer)o);
            }
            if(o instanceof String){
                System.out.println("String:"+(String) o);
            }
        }

    }
}
