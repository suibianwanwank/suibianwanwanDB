package com.ccsu.tb;

import com.ccsu.dm.DataManager;
import com.ccsu.vm.VersionManager;

import java.util.Map;
import java.util.concurrent.locks.Lock;

public class TableManagerBaseCalcite {
    VersionManager vm;

    DataManager dm;

    private Map<String, Table> tableCache;

    Booter booter;

    private Lock lock;



}
