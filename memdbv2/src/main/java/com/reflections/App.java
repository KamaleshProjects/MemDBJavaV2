package com.reflections;

import com.reflections.server.ClientHandler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {

    private static final int PORT = 8080;
    private static final int THREAD_POOL_SIZE = 10;

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) {

        ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server is listening on port::" + PORT);

            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("New Client Connected");

                threadPool.submit(new ClientHandler(socket));
            }
        } catch (IOException ioe) {
            System.out.println("error while trying to listen on port::" + PORT + ioe.getMessage());

        } finally {
            threadPool.shutdown();
        }
    }
}
