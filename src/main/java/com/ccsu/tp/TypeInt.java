package com.ccsu.tp;

import com.ccsu.utils.Parser;

import java.util.Arrays;

public class TypeInt implements Type{
    private final static int TYPE_INT_SIZE=4;

    private int data;

    public TypeInt(Integer data){
        //fixme 可能存在为空不一定
        this.data=data;
    }

    public static Type unParseRaw(byte[] raw) {
        byte[] bytes = Arrays.copyOf(raw, TYPE_INT_SIZE);
        return new TypeInt(Parser.parseInt(bytes));
    }

    public static Type unParseString(String data) {

        return new TypeInt(Integer.valueOf(data));
    }


    public static Type unParse8Raw(byte[] raw) {
        return unParseRaw(Arrays.copyOfRange(raw,4,8));
    }



    public  byte[] parseRaw() {
        return Parser.unParseInt(data);
    }



    public  String parseString() {
        return String.valueOf((int)data);
    }





    @Override
    public int compareTo(Type a) {

        int q1= (int) getData();
        int q2= (int) a.getData();

        if(q1==q2)return 0;
        else if(q1>q2)return 1;

        return -1;




    }


    public  int getSize() {
        return TYPE_INT_SIZE;
    }

    @Override
    public Object getData(){
        return data;
    }
}
