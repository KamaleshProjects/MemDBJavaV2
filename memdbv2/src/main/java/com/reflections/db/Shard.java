package com.reflections.db;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Shard {

    private final ConcurrentHashMap<String, String> data = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public void insert(String key, String value, Transaction txn) {
        this.lock.writeLock().lock();
        try {
            this.data.put(key, value);
            txn.addOperation(() -> this.data.remove(key));
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public String read(String key, Transaction txn) {
        this.lock.readLock().lock();
        try {
            return this.data.get(key);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public void update(String key, String value, Transaction txn) {
        this.lock.writeLock().lock();
        try {
            String oldValue = this.data.put(key, value);
            txn.addOperation(() -> this.data.put(key, oldValue));
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void delete(String key, Transaction txn) {
        this.lock.writeLock().lock();
        try {
            String oldValue = this.data.remove(key);
            txn.addOperation(() -> this.data.put(key, oldValue));
        } finally {
            this.lock.writeLock().unlock();
        }
    }
}
