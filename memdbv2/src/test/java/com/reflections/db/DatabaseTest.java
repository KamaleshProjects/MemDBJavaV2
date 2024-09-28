package com.reflections.db;

import org.junit.jupiter.api.Assertions;
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

        // Before Commit
        Assertions.assertEquals("Alice", usersTable.read("1"));
        Assertions.assertEquals("Bob", usersTable.read("2"));

        transaction.commit();

        // After Commit
        Assertions.assertEquals("Alice", usersTable.read("1"));
        Assertions.assertEquals("Bob", usersTable.read("2"));

        Transaction transaction2 = new Transaction();
        usersTable.update("1", "Alice Updated", transaction2);
        usersTable.delete("2", transaction2);

        // Before Rollback
        Assertions.assertEquals("Alice Updated", usersTable.read("1"));
        Assertions.assertNull(usersTable.read("2"));

        transaction2.rollback();

        // After Rollback
        Assertions.assertEquals("Alice", usersTable.read("1"));
        Assertions.assertEquals("Bob", usersTable.read("2"));
    }
}
