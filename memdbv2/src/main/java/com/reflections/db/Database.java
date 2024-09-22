package com.reflections.db;

import java.util.concurrent.ConcurrentHashMap;

public class Database {

    private final ConcurrentHashMap<String, Table> tables = new ConcurrentHashMap<String, Table>();

    public void createTable(String tableName, int noOfShards) {
        if (tableName == null || tableName.isEmpty()) {
            throw new RuntimeException("tableName cannot be null or empty");
        }
        this.tables.put(tableName, new Table(tableName, noOfShards));
    }

    public void dropTable(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new RuntimeException("tableName cannot be null or empty");
        }
        this.tables.remove(tableName);
    }

    public Table getTable(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new RuntimeException("tableName cannot be null or empty");
        }
        Table maybeTable = this.tables.get(tableName);
        if (maybeTable == null) {
            throw new RuntimeException("table does not exist with tableName::" + tableName);
        }
        return maybeTable;
    }
}
