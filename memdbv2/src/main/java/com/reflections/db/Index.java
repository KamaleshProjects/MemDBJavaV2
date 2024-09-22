package com.reflections.db;

import java.util.concurrent.ConcurrentHashMap;

public class Index {

    private final ConcurrentHashMap<String, Shard> indexMap = new ConcurrentHashMap<>();

    public void add(String key, Shard shard) {
        this.indexMap.put(key, shard);
    }

    public void remove(String key) {
        this.indexMap.remove(key);
    }

    public Shard getShard(String key) {
        return this.indexMap.get(key);
    }
}
