package main;

import dbms.TransactionThread;
import serverlayer.model.InvalidSqlException;
import dbengine.transaction.IsolationLevel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    public static void main(String[] args) throws InvalidSqlException, InterruptedException, ExecutionException {

        BlockingQueue<String> txn1 = new LinkedBlockingQueue<>(), txn2 = new LinkedBlockingQueue<>();

        FutureTask<Void> ft2 = new FutureTask<>(new TransactionThread(IsolationLevel.RR, txn2)),
                ft1 = new FutureTask<Void>(new TransactionThread(IsolationLevel.RR, txn1));
        new Thread(ft1).start(); new Thread(ft2).start();

        txn1.add("insert t values(5,5,5)");
        Thread.sleep(1000);
        txn2.add("select * from t where id > 3 for update");
        Thread.sleep(1000);
        txn1.add("insert t values(6,6,6)");
        Thread.sleep(1000);
        txn1.add("commit");
        txn2.add("commit");
        ft1.get(); ft2.get();
    }

    private static void testRangeSearch(BlockingQueue<String> txn1, BlockingQueue<String> txn2) throws InterruptedException {
        txn1.add("select * from t");
        Thread.sleep(100);
        System.out.println();

        txn1.add("select * from t where name <= \"ccb\"");

        txn1.add("commit");

        Thread.sleep(100);
        System.out.println();

        txn2.add("select * from t where num > 100");

        txn2.add("commit");
    }

    private static void testUpdate(BlockingQueue<String> txn1, BlockingQueue<String> txn2) throws InterruptedException {
        txn1.add("select * from t where id = 1");
        Thread.sleep(100);
        txn2.add("update t set num = 10 where name = \"bbb\"");
        Thread.sleep(100);
        txn1.add("select * from t where id = 2");
        txn2.add("commit");
        Thread.sleep(100);
        txn1.add("select * from t where id = 3");
        txn1.add("commit");
    }

    private static void testSelectForUpdate(BlockingQueue<String> txn1, BlockingQueue<String> txn2) throws InterruptedException {
        txn1.add("select * from t where id = 1 for update");

        txn1.add("select * from t where id = 3 for update");

        Thread.sleep(100);
        txn2.add("select * from t where name = \"bbb\" for update");

        System.out.println("here");
        Thread.sleep(1000);
        txn1.add("commit");

        txn2.add("commit");
    }
}
