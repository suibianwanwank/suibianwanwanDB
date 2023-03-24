package com.ccsu.tp;

import java.util.Objects;

/**
 * 工厂模式管理Type类型
 */
public class TypeFactory {

     enum TYPE{
        INT("int"),
        LONG("long"),
        STRING("string");

        public String getName() {
            return type;
        }

        private final String type;
        TYPE(String type) {
            this.type=type;
        }

        public static TYPE getByFileName(String fileName){
            for(TYPE type:values()){
                if(type.getName().equals(fileName))return type;
            }
            return null;
        }
    }



    public static Type unParseString(String fileName,String data){

        return switch (Objects.requireNonNull(TYPE.getByFileName(fileName))) {
            case INT -> TypeInt.unParseString(data);
            case LONG -> TypeLong.unParseString(data);
            case STRING -> TypeString.unParseString(data);
            default -> null;
        };
    }



    public static Type unParseRaw(String fileName,byte[] data){
        return switch (Objects.requireNonNull(TYPE.getByFileName(fileName))) {
            case INT -> TypeInt.unParseRaw(data);
            case LONG -> TypeLong.unParseRaw(data);
            case STRING -> TypeString.unParseRaw(data);
            default -> null;
        };

    }

    public static Type unParse8Raw(String fileName,byte[] data){
        return switch (Objects.requireNonNull(TYPE.getByFileName(fileName))) {
            case INT -> TypeInt.unParse8Raw(data);
            case LONG -> TypeLong.unParse8Raw(data);
            case STRING -> TypeString.unParse8Raw(data);
            default -> null;
        };

    }



}
