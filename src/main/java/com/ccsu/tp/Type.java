package com.ccsu.tp;

/**
 * 表示filed字段的类型，用TypeFactor统一注册管理
 */
public interface Type {

     byte[] parseRaw();

     String parseString();


     int compareTo(Type a);

     Object getData();

     int getSize();


}
