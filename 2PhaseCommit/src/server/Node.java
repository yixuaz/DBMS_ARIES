package server;

import server.model.TimeoutFuture;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Node {
    public static final String TIMEOUT_RESP = "TIME OUT";
    protected ThreadPoolExecutor threadPool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    private final AtomicBoolean nodeHealth = new AtomicBoolean(true);
    protected final AtomicInteger processingTxnId = new AtomicInteger(-1);

    protected boolean setProcessingJob(int txnId) {
        return processingTxnId.compareAndSet(-1, txnId);
    }

    protected boolean resetProcessingJob(int txnId) {
        return processingTxnId.compareAndSet(txnId, -1);
    }

    protected synchronized boolean waitRebootIfShutDown() throws InterruptedException {
        boolean shutDown = false;
        while (isShutDown()) {
            this.wait();
            shutDown = true;
        }
        return shutDown;
    }

    protected <T> Future<T> doAsync(Callable<T> job) {
        synchronized (nodeHealth) {
            if (isShutDown()) {
                return new TimeoutFuture<>();
            } else {
                return threadPool.submit(job);
            }
        }
    }

    public void shutDown() {
        synchronized (nodeHealth) {
            boolean turnOff = nodeHealth.compareAndSet(true, false);
            assert turnOff;
            threadPool.shutdownNow();
        }
    }

    protected void reboot() {
        synchronized (nodeHealth) {
            if (isShutDown()) {
                threadPool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
                nodeHealth.compareAndSet(false, true);
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }
    }

    public boolean isShutDown() {
        return !nodeHealth.get();
    }

    public boolean hasNoRunningJobs() {
        return threadPool.getTaskCount() == threadPool.getCompletedTaskCount();
    }

    public abstract int successTimes();

    // for test purpose
    public abstract boolean isTxnNotFinished(int txnId);

}
