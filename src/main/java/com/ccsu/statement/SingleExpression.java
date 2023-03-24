package com.ccsu.statement;

public class SingleExpression {
    public String field;
    public String compareOp;
    public String value;

    @Override
    public String toString() {
        return "SingleExpression{" +
                "field='" + field + '\'' +
                ", compareOp='" + compareOp + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
