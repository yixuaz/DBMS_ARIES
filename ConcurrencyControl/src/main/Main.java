package main;

import dbengine.transaction.IsolationLevel;
import dbms.TransactionThread;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        System.out.println("welcome to tiny db isolation level exploration tool");
        System.out.println("please input the client you want to create(eg. 2):");
        ExecutorService es = null;
        try {
            int clientNum = Integer.parseInt(sc.nextLine().trim());
            es = Executors.newFixedThreadPool(clientNum);
            List<BlockingQueue<String>> msgQueues = new ArrayList<>();

            for (int i = 1; i <= clientNum; i++) {
                System.out.println("please input " + i + "th client isolation level (eg. NO, RU, RC, RR, SERIAL):");
                msgQueues.add(new LinkedBlockingQueue<>());
                es.submit(new TransactionThread(IsolationLevel.valueOf(sc.nextLine().trim().toUpperCase())
                        , msgQueues.get(msgQueues.size() - 1)));
            }
            System.out.println("now you can send instruction to client by input 'clientId:sql'");
            System.out.println("(eg. 1:select * from t)");
            System.out.println("table name is t, there are 3 column (id(int)(primary_index), name(string)(non_unique_index), num(int)");
            System.out.println("to see lock info, you can type 'clientId:print lock info'");
            es.shutdown();
            while (!es.isTerminated()) {

                String[] cmd = sc.nextLine().split(":");
                try {
                    int idx = Integer.parseInt(cmd[0]) - 1;
                    String sql = cmd[1];
                    msgQueues.get(idx).add(sql);
                } catch (Exception e) {
                    System.err.println("input error, try again: (eg. 1:select * from t)");
                }
            }
        } catch (Exception e) {
            if (es != null) {
                es.shutdownNow();
            }
        }
    }
}
