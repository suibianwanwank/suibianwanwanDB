package ccsu.oterTest;

import org.junit.Test;

public class ByteTest {
    @Test
    public void test01(){
        long t=(1L<<32)+7;
        int pgno= (int) (t>>32);
        int offset= (int) (t&0x00000000ffffffffL);
        System.out.println(t);
        System.out.println(pgno);
        System.out.println(offset);
        System.out.println(1L<<32);

    }
}
