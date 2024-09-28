package com.reflections.db;

import java.util.ArrayList;
import java.util.List;

public class Table {

    private final List<Shard> shards = new ArrayList<>();
    private final Index index = new Index();

    public Table(String tableName, int noOfShards) {
        for (int i = 0; i < noOfShards; i++) {
            this.shards.add(new Shard());
        }
    }

    public void insert(String key, String value, Transaction txn) {
        Shard shard = this.getShard(key);
        shard.insert(key, value, txn);
        index.add(key, shard);
    }

    public String read(String key) {
        Shard shard = this.getShard(key);
        return shard.read(key);
    }

    public void update(String key, String value, Transaction txn) {
        Shard shard = this.getShard(key);
        shard.update(key, value, txn);
    }

    public void delete(String key, Transaction txn) {
        Shard shard = this.getShard(key);
        shard.delete(key, txn);
    }

    public Shard getShard(String key) {
        int shardIndex = key.hashCode() % this.shards.size();
        return this.shards.get(shardIndex);
    }

}
