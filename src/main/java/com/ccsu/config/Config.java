package com.ccsu.config;

import com.ccsu.utils.Parser;

public class Config {
    public static final int PORT;
    public static final String PATH;
    public static final long MEM_SIZE;



    private static final long DEFAULT_MEM_SIZE = (1<<20)*64;
    private static final int DEFAULT_PORT = 9488;
    private static final String DEFAULT_PATH ="./suibianwanwanDB" ;
//    private static final String DEFAULT_PATH ="D://db//mydata" ;



    static {
        PORT = (YMLConfig.fetchParameter("PORT")==null)?DEFAULT_PORT:Integer.parseInt(YMLConfig.fetchParameter("PORT"));
        PATH=(YMLConfig.fetchParameter("PATH")==null)?DEFAULT_PATH:YMLConfig.fetchParameter("PATH");
        MEM_SIZE=(YMLConfig.fetchParameter("MEM")==null)?DEFAULT_MEM_SIZE:Long.parseLong(YMLConfig.fetchParameter("MEM"));
    }
}
