package dbms;

import dbengine.transaction.IIsolationLevel;
import dbengine.transaction.IsolationLevel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

public class TransactionThread implements Callable<Void> {
    protected BlockingQueue<String> msgQueue;
    protected IIsolationLevel isolationLevel;

    public TransactionThread(IsolationLevel isolationLevel, BlockingQueue<String> msgQueue) {
        this.isolationLevel = isolationLevel.getIIsolationLevel();
        this.msgQueue = msgQueue;
    }

    @Override
    public Void call() throws Exception {
        String msg;
        do {
            msg = msgQueue.take().trim().toLowerCase();
            if (msg.charAt(msg.length() - 1) == ';') {
                msg = msg.substring(0, msg.length() - 1);
            }
            if (!checkSpecialCommandAndDo(msg)) {
                try {
                    DBClient.execute(msg, isolationLevel);
                } catch (Exception e) {
                    System.err.println("invalid sql syntax");
                }
            }
        } while (!"commit".equals(msg));
        return null;
    }

    private boolean checkSpecialCommandAndDo(String msg) {
        if ("print lock info".equals(msg)) {
            isolationLevel.printLockInfo();
            return true;
        }
        return false;
    }
}
