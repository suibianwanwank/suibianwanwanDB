package com.ccsu.statement;

@Deprecated
/*


 */
public class Where {
    public SingleExpression singleExp1;
    public String logicOp;
    public SingleExpression singleExp2;

    @Override
    public String toString() {
        return "Where{" +
                "singleExp1=" + singleExp1 +
                ", logicOp='" + logicOp + '\'' +
                ", singleExp2=" + singleExp2 +
                '}';
    }
}
