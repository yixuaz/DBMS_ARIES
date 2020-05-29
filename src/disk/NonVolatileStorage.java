package disk;

import disk.dbpage.Page;
import disk.wal.LogRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * this class is mock rdbms disk storage
 */
public class NonVolatileStorage {
    private final int totalPages = 10;
    private final List<Page> dataPages;
    private final List<LogRecord> logFiles;
    private int masterRecord;
    public NonVolatileStorage() {
        List<Page> initPages = new ArrayList<>();
        for (int pid = 0; pid < totalPages; pid++) {
            initPages.add(new Page(pid));
        }
        dataPages = Collections.unmodifiableList(initPages);
        logFiles = new ArrayList<>();
    }

    public Page getPage(int pageId) {
        return new Page(dataPages.get(pageId));
    }

    public void flush(LogRecord log) {
        logFiles.add(log);
    }

    public void flush(int pageId, Page page) {
        dataPages.get(pageId).copyPageFrom(page);
    }

    public int getMasterRecord() {
        return masterRecord;
    }

    public void setMasterRecord(int masterRecord) {
        this.masterRecord = masterRecord;
    }

    public Integer getLastLogLsn() {
        return logFiles.isEmpty() ? 0 : logFiles.get(logFiles.size() - 1).getLsn();
    }

    public LogRecord readLog(int lsn) {
        if (lsn > logFiles.size()) {
            return null;
        }
        return logFiles.get(lsn - 1);
    }
}
