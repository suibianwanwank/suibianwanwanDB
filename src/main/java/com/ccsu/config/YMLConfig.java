package com.ccsu.config;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

public class YMLConfig {
    public final static String FILE_PATH="src/main/resources/config.yml";
    public final static Map<String,Object> parameters;

    static {
        Yaml yaml = new Yaml();
        InputStream inputStream= null;
        try {
            inputStream = new FileInputStream(FILE_PATH);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        parameters = yaml.load(inputStream);
        System.out.println(parameters);
    }

    public static String fetchParameter(String parameterName){
        Object o = parameters.get(parameterName);
        if(o!=null){
            return o.toString();
        }
        return null;
    }


}
