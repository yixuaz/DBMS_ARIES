package disk.wal;

import memory.model.ActiveTxn;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * write ahead log data structure
 */
public class LogRecord {
    private final int lsn;

    private int txId;
    private int pageId;
    private int beforeVal;
    private int afterVal;

    private LogType type;
    private Integer prevLsn;
    private Integer undoNextLsn;

    private Map<Integer, ActiveTxn> txId2ActiveTxn;
    private Map<Integer, Integer> dirtyPageIdsTable;

    public LogRecord(int lsn) {
        this.lsn = lsn;
    }

    public void setPrevLsn(Integer prevLsn) {
        this.prevLsn = prevLsn;
    }

    public void setTxId(int txId) {
        this.txId = txId;
    }

    public void setType(LogType type) {
        this.type = type;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    public void setBeforeVal(int beforeVal) {
        this.beforeVal = beforeVal;
    }

    public void setAfterVal(int afterVal) {
        this.afterVal = afterVal;
    }

    public void setUndoNextLsn(Integer undoNextLsn) {
        this.undoNextLsn = undoNextLsn;
    }

    public void setTxId2ActiveTxn(Map<Integer, ActiveTxn> txId2ActiveTxn) {
        this.txId2ActiveTxn = new HashMap<>(txId2ActiveTxn);
    }

    public void setDirtyPageIdsTable(Map<Integer, Integer> dirtyPageIdsTable) {
        this.dirtyPageIdsTable = new HashMap<>(dirtyPageIdsTable);
    }

    public int getLsn() {
        return lsn;
    }

    public Integer getPrevLsn() {
        return prevLsn;
    }

    public int getTxId() {
        return txId;
    }

    public LogType getType() {
        return type;
    }

    public int getPageId() {
        return pageId;
    }

    public int getBeforeVal() {
        return beforeVal;
    }

    public int getAfterVal() {
        return afterVal;
    }

    public Integer getUndoNextLsn() {
        return undoNextLsn;
    }

    public Map<Integer, ActiveTxn> getTxId2ActiveTxn() {
        return Collections.unmodifiableMap(txId2ActiveTxn);
    }

    public Map<Integer, Integer> getDirtyPageIdsTable() {
        return Collections.unmodifiableMap(dirtyPageIdsTable);
    }

    @Override
    public String toString() {
        return "LogRecord{" +
                "lsn=" + lsn +
                ", prevLsn=" + prevLsn +
                ", txId=" + txId +
                ", type=" + type +
                ", pageId=" + pageId +
                ", beforeVal=" + beforeVal +
                ", afterVal=" + afterVal +
                ", undoNextLsn=" + undoNextLsn +
                ", att=" + txId2ActiveTxn +
                ", dpt=" + dirtyPageIdsTable +
                '}';
    }
}
