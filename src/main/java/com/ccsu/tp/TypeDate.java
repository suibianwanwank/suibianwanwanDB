package com.ccsu.tp;
public class TypeDate implements Type{
    @Override
    public byte[] parseRaw() {
        return new byte[0];
    }

    @Override
    public String parseString() {
        return null;
    }

    @Override
    public int compareTo(Type a) {
        return 0;
    }

    @Override
    public Object getData() {
        return null;
    }

    @Override
    public int getSize() {
        return 0;
    }
}
