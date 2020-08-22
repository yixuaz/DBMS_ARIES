package server;

import client.TinyDBClient;
import server.model.TxnResult;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TinyDbShardingNode extends Node implements Participant {
    private final TinyDBClient dbClient;
    private final double abortProbability;
    private final ConcurrentHashMap<Integer, TxnResult> permantStorage; // txnId -> response from this node
    private final TxnCoordinator coordinator;

    public TinyDbShardingNode(double abortProbability, TxnCoordinator coordinator) {
        dbClient = new TinyDBClient();
        this.abortProbability = abortProbability;
        permantStorage = new ConcurrentHashMap<>();
        this.coordinator = coordinator;
    }

    @Override
    public Future<Boolean> canCommit(int txnId, List<Integer> incrementPages) {
        // WARNING: our db client have not support concurrency control, so if one txn not finished,
        // we should not start another txn, otherwise will break Atomicity.
        return doAsync(() -> {
            synchronized (this) {
                if (!setProcessingJob(txnId)) {
                    resolveUnfinishedTask();
                    assert processingTxnId.get() == -1;
                    return false;
                }
                for (int pageId : incrementPages) {
                    if (waitRebootIfShutDown() || Math.random() < abortProbability) {
                        return false;
                    }
                    dbClient.doit("t" + txnId + "-u-p" + pageId);
                }
                dbClient.doit("flush-log");
                assert !permantStorage.containsKey(txnId);
                permantStorage.put(txnId, TxnResult.COMMIT);
                return true;
            }
        });
    }

    public synchronized void resolveUnfinishedTask() {
        int txnId = processingTxnId.get();
        if (txnId != -1) {
            TxnResult status = permantStorage.getOrDefault(txnId, TxnResult.ABORT);
            if (status == TxnResult.ABORT) {
                dbClient.doit("t" + txnId + "-abt");
                assert permantStorage.get(txnId) != TxnResult.COMPLETED_CMT;
                recordTxnProcessingResultInPermantStorage(TxnResult.COMPLETED_ABT);
            } else if (status == TxnResult.COMMIT) {
                TxnResult coordinatorTxnDecision;
                do {
                    coordinatorTxnDecision = queryCoordinatorResult(txnId);
                    if (coordinatorTxnDecision == TxnResult.ABORT) {
                        dbClient.doit("t" + txnId + "-abt");
                        assert permantStorage.get(txnId) != TxnResult.COMPLETED_CMT;
                        recordTxnProcessingResultInPermantStorage(TxnResult.COMPLETED_ABT);
                    } else if (coordinatorTxnDecision == TxnResult.COMMIT) {
                        dbClient.doit("t" + txnId + "-cmt");
                        assert permantStorage.get(txnId) != TxnResult.COMPLETED_ABT;
                        recordTxnProcessingResultInPermantStorage(TxnResult.COMPLETED_CMT);
                    }
                } while (coordinatorTxnDecision == TxnResult.UNKNOWN);
            }
        }
    }

    @Override
    public Future<Boolean> doCommit(int txnId) {
        return doAsync(() -> {
            synchronized (this) {
                assert txnId == processingTxnId.get();
                if (!waitRebootIfShutDown()) {
                    assert permantStorage.get(txnId) == TxnResult.COMMIT;
                    dbClient.doit("t" + txnId + "-cmt");
                    recordTxnProcessingResultInPermantStorage(TxnResult.COMPLETED_CMT);
                    return true;
                } else {
                    return false;
                }
            }
        });
    }

    private void recordTxnProcessingResultInPermantStorage(TxnResult completed) {
        permantStorage.put(processingTxnId.getAndSet(-1), completed);
    }

    @Override
    public Future<Boolean> doAbort(int txnId) {
        // there are two situations in do abort.
        // we receive can commit msg from tc, but crash later. then do abort will have processingTxnId record
        // another is because of crashing period, we failed to receive can commit from tc. then tc wait timeout , it
        // will send do abort, at that time, we back and processingTxnId is -1, it is safe to return true. because we
        // do nothing on this transaction.
        return doAsync(() -> {
            synchronized (this) {
                if (waitRebootIfShutDown()) return false;
                if (processingTxnId.get() == -1) {
                    assert permantStorage.get(txnId) != TxnResult.COMPLETED_CMT;
                    return true;
                }
                assert txnId == processingTxnId.get();
                assert permantStorage.get(txnId) != TxnResult.COMPLETED_CMT;
                dbClient.doit("t" + txnId + "-abt");
                recordTxnProcessingResultInPermantStorage(TxnResult.COMPLETED_ABT);
                return true;
            }
        });
    }

    @Override
    public Future<String> selectResult() {
        return doAsync(() -> {
            synchronized (this) {
                if (!waitRebootIfShutDown()) {
                    return dbClient.doit("slct");
                } else {
                    return null;
                }
            }
        });
    }


    @Override
    public void shutDown() {
        super.shutDown();
        dbClient.crash();
    }

    @Override
    public void reboot() {
        assert isShutDown();
        if (isShutDown()) {
            resolveUnfinishedTask();
            dbClient.start();
            super.reboot();
        }
    }

    private TxnResult queryCoordinatorResult(int txnId) {
        TxnResult coordinatorDecision = TxnResult.UNKNOWN;
        try {
            coordinatorDecision = coordinator.query(txnId).get();
            if (coordinatorDecision == null) coordinatorDecision = TxnResult.UNKNOWN;
            assert coordinatorDecision == TxnResult.UNKNOWN ||
                    coordinatorDecision == TxnResult.COMMIT || coordinatorDecision == TxnResult.ABORT;
        } catch (InterruptedException e) {
            assert false; // invalid area
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            e.printStackTrace(); // invalid area
            assert false;
        }
        return coordinatorDecision;
    }

    @Override
    public synchronized int successTimes() {
        int successTimes = 0;
        for (TxnResult success : permantStorage.values()) {
            successTimes += (success == TxnResult.COMPLETED_CMT ? 1 : 0);
        }
        return successTimes;
    }

    @Override
    public boolean isTxnNotFinished(int txnId) {
        return !permantStorage.getOrDefault(txnId, TxnResult.COMPLETED_ABT).isTerminalStatus();
    }
}
