package dbms;

import disk.NonVolatileStorage;
import disk.wal.LogRecord;
import disk.wal.LogType;
import memory.Memory;
import memory.model.ActiveTxn;
import memory.model.TxnStatus;

import java.util.HashMap;
import java.util.Map;

public class LogManager {
    private final Memory memory;
    private final NonVolatileStorage disk;

    public LogManager(Memory memory, NonVolatileStorage disk) {
        this.memory = memory;
        this.disk = disk;
    }

    public int appendUpdateLog(int beforeVal, int afterVal, int txId, int pageId, Integer prevLsn) {
        LogRecord log = new LogRecord(memory.nextLsn());
        log.setBeforeVal(beforeVal);
        log.setAfterVal(afterVal);
        log.setType(LogType.UPDATE);
        log.setPageId(pageId);
        log.setPrevLsn(prevLsn);
        log.setTxId(txId);
        memory.addLog(log);
        return log.getLsn();
    }


    public void appendCheckpointStartLog() {
        LogRecord log = new LogRecord(memory.nextLsn());
        log.setType(LogType.CP_START);
        memory.addLog(log);
    }

    public int appendCheckpointEndLog() {
        int idx = disk.getMasterRecord() + 1;
        LogRecord cur;
        Map<Integer, Integer> dpt = new HashMap<>();
        Map<Integer, ActiveTxn> att = new HashMap<>();
        Map<Integer, ActiveTxn> tmpAtt = new HashMap<>();
        int cpStartLsn = -1;
        memory.flushLog();
        while ((cur = disk.readLog(idx)) != null) {
            final int curLsn = idx, txId = cur.getTxId(), pid = cur.getPageId();
            if (cur.getType() == LogType.UPDATE || cur.getType() == LogType.CLR) {
                dpt.put(pid, memory.getDataPage(pid).getRecLSN());
                if (cpStartLsn < 0) {
                    att.compute(cur.getTxId(), (k, v) -> {
                        if (v == null) return new ActiveTxn(txId, TxnStatus.Running, curLsn);
                        v.lastLsn = curLsn;
                        return v;
                    });
                }
            } else if (cur.getType() == LogType.COMMIT || cur.getType() == LogType.ABORT){
                att.compute(cur.getTxId(), (k, v) -> {
                    if (v == null) return new ActiveTxn(txId, TxnStatus.Committing, curLsn);
                    v.status = TxnStatus.Committing;
                    v.lastLsn = curLsn;
                    return v;
                });
            } else if (cur.getType() == LogType.TX_END) {
                att.remove(cur.getTxId());
            } else if (cur.getType() == LogType.CP_START) {
                cpStartLsn = cur.getLsn();
            }
            idx++;
        }
        appendCheckpointEndLog(dpt, att);
        return Math.max(0, cpStartLsn);
    }

    private void appendCheckpointEndLog(Map<Integer, Integer> dpt, Map<Integer, ActiveTxn> att) {
        LogRecord log = new LogRecord(memory.nextLsn());
        log.setType(LogType.CP_END);
        log.setDirtyPageIdsTable(dpt);
        log.setTxId2ActiveTxn(att);
        memory.addLog(log);
    }

    public int appendCLR(LogRecord oriLog, Integer prevLsn) {
        LogRecord log = new LogRecord(memory.nextLsn());
        log.setType(LogType.CLR);
        log.setTxId(oriLog.getTxId());
        log.setPrevLsn(prevLsn);
        log.setPageId(oriLog.getPageId());
        log.setAfterVal(oriLog.getBeforeVal());
        log.setBeforeVal(oriLog.getAfterVal());
        log.setUndoNextLsn(oriLog.getPrevLsn());
        memory.addLog(log);
        return log.getLsn();
    }

    public int appendCommitLog(int txId, Integer prevLsn) {
        return appendTxnLog(txId, prevLsn, LogType.COMMIT);
    }

    public int appendAbortLog(int txId,  Integer prevLsn) {
        return appendTxnLog(txId, prevLsn, LogType.ABORT);
    }

    public int appendTxnEndLog(int txId, Integer prevLsn) {
        return appendTxnLog(txId, prevLsn, LogType.TX_END);
    }

    private int appendTxnLog(int txId, Integer prevLsn, LogType type) {
        LogRecord log = new LogRecord(memory.nextLsn());
        log.setTxId(txId);
        log.setType(type);
        log.setPrevLsn(prevLsn);
        memory.addLog(log);
        return log.getLsn();
    }



}
