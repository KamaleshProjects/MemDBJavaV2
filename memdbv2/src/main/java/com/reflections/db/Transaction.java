package com.reflections.db;

import java.util.ArrayList;
import java.util.List;

public class Transaction {

    private final List<Runnable> operationList = new ArrayList<>();
    private boolean committed = false;

    public void addOperation(Runnable operation) {
        if (!this.committed) {
            this.operationList.add(operation);
        }
    }

    public void commit() {
        committed = true;
        this.operationList.clear();
    }

    public void rollback() {
        for (Runnable operation: this.operationList) {
            operation.run();
        }
        this.operationList.clear();
    }
}
