package com.ccsu;

import com.ccsu.config.Config;
import com.ccsu.config.YMLConfig;
import com.ccsu.dm.DataManager;
import com.ccsu.dm.DataManagerImpl;
import com.ccsu.http.HttpServer;
import com.ccsu.log.RedoLogManager;
import com.ccsu.log.UndoLogManager;
import com.ccsu.tb.Booter;
import com.ccsu.tb.TableManager;
import com.ccsu.tb.TableManagerImpl;
import com.ccsu.tm.TransactionManager;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.vm.VersionManager;
import com.ccsu.vm.VersionManagerImpl;
import org.yaml.snakeyaml.Yaml;

import java.io.File;

public class Start {




    public static void main(String[] args) throws Exception {
        HttpServer server=new HttpServer(init(),Config.PORT);
        server.start();
    }

    public static TableManager create() throws Exception {
        UndoLogManager undoLogManager = UndoLogManager.create(Config.PATH);
        RedoLogManager redoLogManager = RedoLogManager.create(Config.PATH);
        DataManager dm = DataManagerImpl.create(Config.PATH, Config.MEM_SIZE,redoLogManager);
        TransactionManager tm = TransactionManagerImpl.create(Config.PATH);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        return TableManagerImpl.create(vm,Config.PATH, dm);

    }

    public static TableManager open() throws Exception {
        UndoLogManager undoLogManager = UndoLogManager.open(Config.PATH);
        RedoLogManager redoLogManager = RedoLogManager.open(Config.PATH);
        TransactionManager tm = TransactionManagerImpl.open(Config.PATH);
        DataManager dm = DataManagerImpl.open(Config.PATH,tm,Config.MEM_SIZE,redoLogManager);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        return TableManagerImpl.create(vm,Config.PATH, dm);
    }

    public static TableManager init(){
        try {
            File file=new File(Config.PATH+ Booter.BOOTER_SUFFIX);
            if(!file.exists()){
                return create();
            }else{
                return open();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
