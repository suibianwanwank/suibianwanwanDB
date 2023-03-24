package com.ccsu;

import com.ccsu.dm.DataManager;
import com.ccsu.dm.DataManagerImpl;
import com.ccsu.http.HttpServer;
import com.ccsu.log.RedoLogManager;
import com.ccsu.log.UndoLogManager;
import com.ccsu.tb.TableManager;
import com.ccsu.tb.TableManagerImpl;
import com.ccsu.tm.TransactionManager;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.vm.VersionManager;
import com.ccsu.vm.VersionManagerImpl;

public class Start {

    public static final long DEFAULT_MEM = (1<<20)*64;

    public static void main(String[] args) throws Exception {

        TableManager tbm=create();
        HttpServer server=new HttpServer(tbm,8080);
        server.start();

    }

    public static TableManager create() throws Exception {
        String path="D://db//mydata";
        UndoLogManager undoLogManager = UndoLogManager.create(path);
        RedoLogManager redoLogManager = RedoLogManager.create(path);
        DataManager dm = DataManagerImpl.create(path, DEFAULT_MEM,redoLogManager);
        TransactionManager tm = TransactionManagerImpl.create(path);
        VersionManager vm = new VersionManagerImpl(tm, dm,undoLogManager);
        TableManager tbm = TableManagerImpl.create(vm,path, dm);
        return tbm;
    }
}
