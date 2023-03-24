package com.ccsu.tp;

import com.ccsu.utils.Bytes;
import com.ccsu.utils.Parser;

import java.util.Arrays;

public class TypeString implements Type{

    String data;

    public TypeString(String data) {
        this.data = data;
    }

    public static Type unParseRaw(byte[] raw) {
        int length = Parser.parseInt(Arrays.copyOfRange(raw, 0, 4));
        return new TypeString(new String(Arrays.copyOfRange(raw,4,length+4)));
    }

    public static Type unParseString(String data) {

        return new TypeString(data);
    }


    public static Type unParse8Raw(byte[] raw) {
        return unParseRaw(Arrays.copyOfRange(raw,0,8));
    }


    @Override
    public byte[] parseRaw() {
        byte[] bytes = Parser.unParseInt(data.length());

        return Bytes.concat(bytes,data.getBytes());
    }

    @Override
    public String parseString() {
        return data;
    }

    @Override
    public int compareTo(Type a) {
        String a1= (String) getData();
        String a2= (String) a.getData();

        return a1.compareTo(a2);
    }


    public Object getData() {
        return data;
    }

    public int getSize() {
        return data.length()+4;
    }
}
