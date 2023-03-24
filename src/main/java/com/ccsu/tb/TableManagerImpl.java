package com.ccsu.tb;

import com.ccsu.dm.DataManager;
import com.ccsu.statement.*;
import com.ccsu.utils.Parser;
import com.ccsu.vm.VersionManager;
import lombok.extern.slf4j.Slf4j;
import com.ccsu.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
@Slf4j
public class TableManagerImpl implements TableManager{

    VersionManager vm;

    DataManager dm;

    private Map<String, Table> tableCache;

    Booter booter;

    private Lock lock;

    public TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.dm=dm;
        this.vm=vm;
        this.booter=booter;
        this.tableCache = new HashMap<>();
        lock=new ReentrantLock();
        loadTables();
    }


    public static TableManager create(VersionManager vm,String path, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.unParseLong(0));
        return new TableManagerImpl(vm,dm, booter);
    }

    public static TableManager open(VersionManager vm,String path, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm,dm, booter);
    }




    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            Table table = Table.createTable(this,firstTableUid(), xid, create,dm);

            updateFirstTableUid(table.uid);

            tableCache.put(create.tableName, table);

            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }


    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(xid, read).getBytes();
    }

    @Override
    public byte[] rollback(long xid) throws Exception {
        vm.abort(xid);
        return "abort".getBytes();
    }

    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table=tableCache.get(update.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(xid, update);

        return Parser.unParseInt(count);
    }

    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }



    private Long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if(table == null) {
            log.error("表不存在");
            return null;
        }
        table.insert(xid, insert);
        return "insert".getBytes();
    }

    @Override
    public long begin(Begin begin) {
        //区分隔离级别
        int level= begin.isolationLevel;
        return vm.begin(level);

    }

    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }



    private void updateFirstTableUid(long uid) {

        //TODO 更新table firstuid

        byte[] raw = Parser.unParseLong(uid);
        booter.update(raw);
    }

    private void loadTables() {
        long uid = firstTableUid();
        log.info("uid的值是：{}",uid);
        while(uid != 0) {
            Table tb = Table.loadTable(this, uid,dm);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }

}
