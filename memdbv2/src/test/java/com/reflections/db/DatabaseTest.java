package com.reflections.db;

import org.junit.jupiter.api.Test;

public class DatabaseTest {

    @Test
    public void testDatabase() {
        Database db = new Database();
        db.createTable("users", 4);

        Table usersTable = db.getTable("users");

        Transaction transaction = new Transaction();
        usersTable.insert("1", "Alice", transaction);
        usersTable.insert("2", "Bob", transaction);

        System.out.println("Before Commit:");
        System.out.println("User 1: " + usersTable.read("1", transaction));
        System.out.println("User 2: " + usersTable.read("2", transaction));

        transaction.commit();

        System.out.println("After Commit:");
        System.out.println("User 1: " + usersTable.read("1", null));
        System.out.println("User 2: " + usersTable.read("2", null));

        Transaction transaction2 = new Transaction();
        usersTable.update("1", "Alice Updated", transaction2);
        usersTable.delete("2", transaction2);

        System.out.println("Before Rollback:");
        System.out.println("User 1: " + usersTable.read("1", transaction2));
        System.out.println("User 2: " + usersTable.read("2", transaction2));

        transaction2.rollback();

        System.out.println("After Rollback:");
        System.out.println("User 1: " + usersTable.read("1", null));
        System.out.println("User 2: " + usersTable.read("2", null));
    }
}
