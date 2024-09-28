package com.reflections.server;

import com.reflections.db.Database;
import com.reflections.db.Table;
import com.reflections.db.Transaction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.StringTokenizer;

public class ClientHandler implements Runnable {

    private final Socket socket;
    private final Database database;

    public ClientHandler(Socket socket) {
        this.socket = socket;
        this.database = new Database();
    }

    private static final String DELIM = " ";
    private static final int NUM_SHARDS = 4;
    private static final String CREATE = "create";
    private static final String DELETE = "delete";
    private static final String READ = "read";
    private static final String INSERT = "insert";
    private static final String UPDATE = "update";

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream())) {

            // Read HTTP request from the client
            String requestLine = in.readLine();
            if (requestLine != null && !requestLine.isEmpty()) {
                System.out.println("Request: " + requestLine);

                StringTokenizer input = new StringTokenizer(requestLine, DELIM);
                int numTokens = input.countTokens();

                if (numTokens == 2) {
                    String cmdToken = input.nextToken();
                    String argToken = input.nextToken();
                    if (CREATE.equals(cmdToken)) {
                        // create table_name
                        this.database.createTable(argToken, NUM_SHARDS);
                        out.println("table::" + argToken + " created");
                        return;
                    }
                    if (DELETE.equals(cmdToken)) {
                        // delete table_name
                        this.database.dropTable(argToken);
                        out.println("table::" + argToken + " deleted");
                        return;
                    }
                }

                if (numTokens == 3) {
                    String cmdToken = input.nextToken();
                    String tableArgToken = input.nextToken();
                    String keyArgToken = input.nextToken();

                    if (READ.equals(cmdToken)) {
                        // read table_name key
                        Table table = this.database.getTable(tableArgToken);
                        if (table == null) {
                            out.println("table::" + tableArgToken + " not found");
                            return;
                        }
                        String value = table.read(keyArgToken);
                        out.println(value);
                        return;
                    }
                    if (DELETE.equals(cmdToken)) {
                        // delete table_name key
                        Table table = this.database.getTable(tableArgToken);
                        if (table == null) {
                            out.println("table::" + tableArgToken + " not found");
                            return;
                        }
                        Transaction txn = new Transaction();
                        table.delete(keyArgToken, txn);
                        txn.commit();
                        out.println("key::" + keyArgToken + " from table::" + table + " is deleted");
                        return;
                    }
                }

                if (numTokens == 4) {
                    String cmdToken = input.nextToken();
                    String tableArgToken = input.nextToken();
                    String keyArgToken = input.nextToken();
                    String valueArgToken = input.nextToken();

                    Transaction txn = new Transaction();
                    if (INSERT.equals(cmdToken)) {
                        // insert table_name key value
                        Table table = this.database.getTable(tableArgToken);
                        if (table == null) {
                            out.println("table::" + tableArgToken + " not found");
                            return;
                        }
                        table.insert(keyArgToken, valueArgToken, txn);
                        txn.commit();
                        out.println(
                                "value::" + valueArgToken + " inserted for key::" + keyArgToken + " in table::" +
                                        tableArgToken
                        );
                        return;
                    }
                    if (UPDATE.equals(cmdToken)) {
                        // update table_name key value
                        Table table = this.database.getTable(tableArgToken);
                        if (table == null) {
                            out.println("table::" + tableArgToken + " not found");
                            return;
                        }
                        table.update(keyArgToken, valueArgToken, txn);
                        txn.commit();
                        out.println(
                                "value::" + valueArgToken + " updated for key::" + keyArgToken + " in table::" +
                                        tableArgToken
                        );
                        return;
                    }
                }

                out.println("Unsupported request::" + requestLine);
            }

        } catch (IOException e) {
            System.out.println("Client handler error: " + e.getMessage());

        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                System.out.println("Error closing socket: " + e.getMessage());
            }
        }
    }
}
