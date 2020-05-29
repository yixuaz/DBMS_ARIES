package solution;

import dbms.LogManager;
import disk.NonVolatileStorage;
import disk.dbpage.Page;
import disk.wal.LogRecord;
import disk.wal.LogType;
import memory.Memory;
import memory.model.ActiveTxn;
import memory.model.TxnStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecoveryManager {
    private static final double STEP_CRASH_POSSIBILY = 0.08;
    private final NonVolatileStorage disk;
    private final Memory bufferPool;
    private final LogManager logManager;
    private final Map<Integer, ActiveTxn> att;
    private final Map<Integer, Integer> dpt;

    private final boolean enableDebugInfo = false;
    private final double crashPossibilityDuringRecover;

    public RecoveryManager(NonVolatileStorage disk, Memory bufferPool, LogManager logManager
            , double possibilityCrashInRecoverSteps) {
        this.disk = disk;
        this.bufferPool = bufferPool;
        this.logManager = logManager;
        att = new HashMap<>();
        dpt = new HashMap<>();
        this.crashPossibilityDuringRecover = possibilityCrashInRecoverSteps;
    }

    public boolean recover() {
        try {
            analysis();
            redo();
            undo();
            return true;
        } catch (CrashException e) {
            System.err.println(e.getMessage());
            bufferPool.clear();
            return false;
        }
    }

    /**
     * implement ARIES analysis method here
     * <p>
     * Start from last checkpoint found via the database’s MasterRecord LSN.
     * • Scan log forward from the checkpoint.
     * • If the DBMS finds a TXN-END record, remove its transaction from ATT.
     * • All other records, add transaction to ATT with status UNDO, and on commit, change transaction
     * status to COMMIT.
     * • For UPDATE log records, if page P is not in the DPT, then add P to DPT and set P’srecLSN to the log
     * record’s LSN.
     */
    public void analysis() throws CrashException {
        int idx = disk.getMasterRecord() + 1;
        LogRecord cur;
        boolean shouldCrash = Math.random() < crashPossibilityDuringRecover;
        while ((cur = disk.readLog(idx)) != null) {
            if (shouldCrash && Math.random() < STEP_CRASH_POSSIBILY) {
                throw new CrashException("crash during analysis");
            }
            if (cur.getType() == LogType.CP_END) {
                for (Map.Entry<Integer, Integer> e : cur.getDirtyPageIdsTable().entrySet()) {
                    dpt.putIfAbsent(e.getKey(), e.getValue());
                }
                for (Map.Entry<Integer, ActiveTxn> e : cur.getTxId2ActiveTxn().entrySet()) {
                    ActiveTxn at = new ActiveTxn(e.getValue());
                    at.status = TxnStatus.Undo;
                    att.putIfAbsent(e.getKey(), at);
                }
            } else if (cur.getType() == LogType.UPDATE || cur.getType() == LogType.CLR) {
                Page page = bufferPool.getDataPage(cur.getPageId());
                int txId = cur.getTxId(), lsn = cur.getLsn();
                att.compute(txId, (k, v) -> {
                    if (v == null) return new ActiveTxn(txId, TxnStatus.Undo, lsn);
                    v.lastLsn = lsn;
                    return v;
                });
                if (cur.getType() == LogType.UPDATE && !dpt.containsKey(cur.getPageId())) {
                    dpt.put(cur.getPageId(), cur.getLsn());
                    page.setRecLsn(cur.getLsn());
                }
            } else if (cur.getType() == LogType.TX_END) {
                att.remove(cur.getTxId());
            } else if (cur.getType() == LogType.COMMIT) {
                int txId = cur.getTxId(), lsn = cur.getLsn();
                att.compute(txId, (k, v) -> {
                    if (v == null) return new ActiveTxn(txId, TxnStatus.Committing, lsn);
                    v.lastLsn = lsn;
                    v.status = TxnStatus.Committing;
                    return v;
                });
            }
            idx++;
        }
        if (shouldCrash) {
            throw new CrashException("crash after analysis");
        }
        if (enableDebugInfo) {
            System.out.println(att);
            System.out.println(dpt);
        }

    }

    /**
     * implement redo method here
     * <p>
     * The goal is to repeat history to reconstruct state at the moment of the crash. Reapply all updates (even
     * aborted transactions) and redo CLRs.
     * The DBMS scans forward from log record containing smallest recLSN in the DPT. For each update log
     * record or CLR with a given LSN, the DBMS re-applies the update unless:
     * • Affected page is not in the DPT, or
     * • Affected page is in DPT but that record’s LSN is greater than smallest recLSN, or
     * • Affected pageLSN (on disk) ≥ LSN.
     * To redo an action, the DBMS re-applies the change in the log record and then sets the affected page’s
     * pageLSN to that log record’s LSN.
     * At the end of the redo phase, write TXN-END log records for all transactions with status COMMIT and remove
     * them from the ATT.
     */
    public void redo() throws CrashException {
        int minLsn = Integer.MAX_VALUE;
        for (int lsn : dpt.values()) {
            minLsn = Math.min(minLsn, lsn);
        }
        LogRecord cur;
        boolean shouldCrash = Math.random() < crashPossibilityDuringRecover;
        for (int i = minLsn; (cur = disk.readLog(i)) != null; i++) {
            if (shouldCrash && Math.random() < STEP_CRASH_POSSIBILY) {
                throw new CrashException("crash during redo");
            }
            if (cur.getType() == LogType.UPDATE || cur.getType() == LogType.CLR) {
                if (!dpt.containsKey(cur.getPageId())) continue;
                Page page = bufferPool.getDataPage(cur.getPageId());
                if (page.getPageLSN() != null && page.getPageLSN() >= cur.getLsn()) continue;
                page.update(cur.getAfterVal(), cur.getLsn());
            }
        }
        List<Integer> toBeRemoved = new ArrayList<>();
        for (ActiveTxn at : att.values()) {
            if (at.status == TxnStatus.Committing) {
                logManager.appendTxnEndLog(at.txId, at.lastLsn);
                toBeRemoved.add(at.txId);
            }
        }
        for (int tid : toBeRemoved) att.remove(tid);
        if (shouldCrash) {
            throw new CrashException("crash after redo");
        }
        if (enableDebugInfo) {
            System.out.println(att);
            System.out.println(dpt);
        }
    }

    /**
     * implement undo method here
     * <p>
     * In the last phase, the DBMS reverses all transactions that were active at the time of crash. These are all
     * transactions with UNDO status in the ATT after the Analysis phase.
     * The DBMS processes transactions in reverse LSN order using the lastLSN to speed up traversal. As it
     * reverses the updates of a transaction, the DBMS writes a CLR entry to the log for each modification.
     * Once the last transaction has been successfully aborted, the DBMS flushes out the log and then is ready to
     * start processing new transactions.
     */
    public void undo() throws CrashException {
        boolean shouldCrash = Math.random() < crashPossibilityDuringRecover;
        for (ActiveTxn at : att.values()) {
            Integer prevLsn = null;
            for (Integer i = at.lastLsn; i != null; ) {
                if (shouldCrash && Math.random() < STEP_CRASH_POSSIBILY) {
                    throw new CrashException("crash during undo");
                }
                LogRecord cur = disk.readLog(i);
                if (prevLsn == null) prevLsn = cur.getLsn();
                if (cur.getType() == LogType.CLR) {
                    i = cur.getUndoNextLsn();
                } else if (cur.getType() == LogType.UPDATE) {
                    Page page = bufferPool.getDataPage(cur.getPageId());
                    page.update(cur.getBeforeVal(), page.getPageLSN());
                    prevLsn = logManager.appendCLR(cur, prevLsn);
                    bufferPool.flushLog();
                    i = cur.getPrevLsn();
                } else {
                    throw new IllegalStateException(cur.toString());
                }
            }
            logManager.appendTxnEndLog(at.txId, prevLsn);
            bufferPool.flushPage();
        }
        if (shouldCrash) {
            throw new CrashException("crash after undo");
        }
    }

    private class CrashException extends Exception {
        public CrashException(String message) {
            super(message);
        }
    }
}
