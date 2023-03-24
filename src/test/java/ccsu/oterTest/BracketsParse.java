package ccsu.oterTest;

import com.ccsu.statement.SingleExpression;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class BracketsParse {

    @Test
    public void test01() {
        Stack<Integer> stack = new Stack<>();

        String x = "where x>10 and (y<5 or (x>10 or y<2))and x>2;";
                /*
                    如果遇到“）”出栈

                 */

    }

    static class Area {
        String name;
        int left;
        int right;
    }

    private static List<Area> areas=new ArrayList<>();

    public static void parse(int pos, String x) {
        List<Area> tempArea=new ArrayList<>();
        SingleExpression singleExpression=new SingleExpression();

        char c = x.charAt(pos);
        pos++;
        while (true) {
            if(c=='('){
              parse(pos,x);
            }else if(c==')'){
                //计算括号里的结果返回


                areas.addAll(tempArea);

                return ;
            }

        }


    }
}
