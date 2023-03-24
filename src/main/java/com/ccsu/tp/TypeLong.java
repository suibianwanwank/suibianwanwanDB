package com.ccsu.tp;


import com.ccsu.utils.Parser;

import java.util.Arrays;

public class TypeLong implements Type{

    private Long data;

    private final static int TYPE_LONG_SIZE=8;

    public TypeLong(Long data) {
        if(data==null)return ;
        this.data=data;
    }






    public static  Type unParseString(String data) {
        return new TypeLong(Long.parseLong(data));
    }



    public static Type unParseRaw(byte[] raw) {
        byte[] bytes = Arrays.copyOf(raw, TYPE_LONG_SIZE);
        return new TypeLong(Parser.parseLong(bytes));

    }


    public static Type unParse8Raw(byte[] raw) {
        return unParseRaw(raw);
    }

    @Override
    public String parseString() {
        return String.valueOf(data);
    }

    @Override
    public byte[] parseRaw() {
        return Parser.unParseLong(data);
    }



    public  int getSize() {
        return TYPE_LONG_SIZE;
    }




    @Override
    public int compareTo(Type a) {
        long q1= (long) getData();
        long q2= (long) a.getData();
        if(q1==q2)return 0;
        else if(q1>q2)return 1;

        return -1;

    }

    @Override
    public Object getData() {
        return data;
    }
}
