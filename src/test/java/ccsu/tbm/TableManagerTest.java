package ccsu.tbm;

import org.junit.Test;

public class TableManagerTest {


    @Test
    public void Test01(){
//        Integer a=12222;
////        Integer b=12222;
//        Student a=new Student();
//        Student b=new Student();


        String b=new String("123");

        String a=b.intern();
//        String b=a;

        System.out.println(a==b);
        System.out.println(a.equals(b));
    }

    @Test
    public void test02(){
        Integer a=Integer.valueOf(3);
        Integer b=1;
        System.out.println(a==b);
        System.out.println(a.equals(b));

    }
}
