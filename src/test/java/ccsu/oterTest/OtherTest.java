package ccsu.oterTest;

import java.util.LinkedList;

public class OtherTest {
    public static void main(String[] args) {

        LinkedList<String> list=new LinkedList<>();

        list.add("xxx1");
        list.add("xxx2");
        list.add(0,"tt");
        list.remove(1);
        list.remove(1);
        list.forEach(System.out::println);


    }
}
