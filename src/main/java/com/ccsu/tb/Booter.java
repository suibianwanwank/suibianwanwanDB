package com.ccsu.tb;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

@Slf4j
public class Booter {
    public static final String BOOTER_SUFFIX=".bt";

    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    public Booter(String path, File file) {
        this.path=path;
        this.file=file;
    }

    public static Booter create(String path){
        File file=new File(path+BOOTER_SUFFIX);
        try {
            boolean newFile = file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new Booter(path,file);
    }

    public void update(byte[] data) {
        File tmp = new File(path + BOOTER_TMP_SUFFIX);

        try {
            tmp.createNewFile();
        } catch (Exception e) {
            log.error("创建tmp失败");
        }

        try(FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch(IOException e) {
            log.error("写入失败");
        }
        try {
            Files.move(tmp.toPath(), new File(path+BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.error("转移失败");
        }


    }

    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buf;
    }

    public static Booter open(String path) {
//        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        if(!f.exists()) {
            log.error("boot文件不存在发生错误");
        }
        if(!f.canRead() || !f.canWrite()) {
            log.error("权限不足，boot文件无法读写");
        }
        return new Booter(path, f);
    }
}
