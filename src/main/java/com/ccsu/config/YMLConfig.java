package com.ccsu.config;

import com.ccsu.Start;
import com.ccsu.bp.BPlusTree;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class YMLConfig {
    private final static String FILE_PATH="src/main/resources/config.yml";
    public final static Map<String,Object> parameters;

    static {
        Yaml yaml = new Yaml();
        InputStream inputStream = BPlusTree.class.getClassLoader().getResourceAsStream("config.yml");
        parameters = yaml.load(inputStream);

    }

    public static String fetchParameter(String parameterName){
        Object o = parameters.get(parameterName);
        if(o!=null){
            return o.toString();
        }
        return null;
    }
}
