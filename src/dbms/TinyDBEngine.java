package dbms;

import disk.NonVolatileStorage;
import disk.dbpage.Page;
import disk.wal.LogRecord;
import memory.Memory;

public class TinyDBEngine {
    private final NonVolatileStorage disk;
    private final Memory bufferPool;
    private final LogManager logManager;
    private final int totalPages = 10;

    public TinyDBEngine() {
        disk = new NonVolatileStorage();
        bufferPool = new Memory(disk);
        logManager = new LogManager(bufferPool, disk);
    }

    public int increment(int pageId, int txId, Integer prevLsn) {
        Page page = bufferPool.getDataPage(pageId);
        int oldVal = page.getData();
        int lsn = logManager.appendUpdateLog(oldVal, oldVal + 1, txId, pageId, prevLsn);
        page.update(oldVal + 1, lsn);
        return lsn;
    }

    public void startCheckpoint() {
        logManager.appendCheckpointStartLog();
    }

    public void endCheckpoint() {
        int startCheckpointLsn = logManager.appendCheckpointEndLog();
        disk.setMasterRecord(startCheckpointLsn);
        bufferPool.flushPage();
    }

    public int commit(int txid, Integer prevLsn) {
        int lsn = logManager.appendCommitLog(txid, prevLsn);
        bufferPool.flushLog();
        return lsn;
    }

    public int abort(int txid, Integer prevLsn) {
        int curLsn = logManager.appendAbortLog(txid, prevLsn);
        bufferPool.flushLog();
        Integer pre = prevLsn;
        while (pre != null) {
            LogRecord log = disk.readLog(pre);
            Page page = bufferPool.getDataPage(log.getPageId());
            page.update(log.getBeforeVal(), curLsn);
            curLsn = logManager.appendCLR(log, curLsn);
            pre = log.getPrevLsn();
        }
        return curLsn;
    }

    public void flushLog() {
        // this function is used for lab3
        bufferPool.flushLog();
    }
    public void txnEnd(int txid, Integer prevLsn) {
        logManager.appendTxnEndLog(txid, prevLsn);
    }

    public void flush() {
        bufferPool.flushPage();
    }

    public String selectAll() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int pid = 0; pid < totalPages; pid++) {
            stringBuilder.append(", ");
            stringBuilder.append(bufferPool.getDataPage(pid).getData());
        }
        return stringBuilder.toString().substring(2);
    }

    public String showLog() {
        bufferPool.flushLog();
        StringBuilder stringBuilder = new StringBuilder();
        int lsn = 1;
        LogRecord cur;
        while ((cur = disk.readLog(lsn)) != null) {
            stringBuilder.append(cur).append("\n");
            lsn++;
        }
        return stringBuilder.toString();
    }

    public void crash() {
        bufferPool.clear();
    }

    public boolean recover(double crashPossibiltyDuringRecover) {
        RecoveryManager rm = new RecoveryManager(disk, bufferPool, logManager, crashPossibiltyDuringRecover);
        boolean isSuccess = rm.recover();
        bufferPool.flushLog();
        return isSuccess;
    }
}
