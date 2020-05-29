package dbms;

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
        // TODO: ADD YOUR CODE HERE
        // be careful in this method should have crashPossibilityDuringRecover to throw CrashException
        // to pass the last test case

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
        // TODO: ADD YOUR CODE HERE
        // be careful in this method should have crashPossibilityDuringRecover to throw CrashException
        // to pass the last test case
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
        // TODO: ADD YOUR CODE HERE
        // be careful in this method should have crashPossibilityDuringRecover to throw CrashException
        // to pass the last test case
    }

    private class CrashException extends Exception {
        public CrashException(String message) {
            super(message);
        }
    }
}
