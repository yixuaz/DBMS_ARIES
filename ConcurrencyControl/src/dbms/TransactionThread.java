package dbms;

import dbengine.transaction.IIsolationLevel;
import dbengine.transaction.IsolationLevel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

public class TransactionThread implements Callable<Void> {
    BlockingQueue<String> msgQueue;
    IIsolationLevel isolationLevel;

    public TransactionThread(IsolationLevel isolationLevel, BlockingQueue<String> msgQueue) {
        this.isolationLevel = isolationLevel.getIIsolationLevel();
        this.msgQueue = msgQueue;
    }

    @Override
    public Void call() throws Exception {
        String msg;
        do {
            msg = msgQueue.take().trim();
            DBClient.doit(msg, isolationLevel);
        } while (!"commit".equals(msg));
        return null;
    }
}
