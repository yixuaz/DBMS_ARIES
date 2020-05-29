package memory;

import disk.NonVolatileStorage;
import disk.dbpage.Page;
import disk.wal.LogRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * this class is mock rdbms memory storage
 */
public class Memory {
    private final Map<Integer, Page> pageId2Page = new HashMap<>();
    private final List<LogRecord> writeAheadLogs = new ArrayList<>();
    private final NonVolatileStorage disk;
    private int flushedLsn = 0;
    private AtomicInteger lsnProducer = new AtomicInteger(0);

    public Memory(NonVolatileStorage disk) {
        this.disk = disk;
    }

    /**
     * copyPageFrom dirty page into disk
     */
    public void flushPage() {
        flushLog();
        List<Integer> toBeDeleted = new ArrayList<>();
        for (Map.Entry<Integer, Page> entry : pageId2Page.entrySet()) {
            int pageId = entry.getKey();
            Page page = entry.getValue();
            // Before page i can be written to disk, we must copyPageFrom log at least to the point where pageLSNi ≤ flushedLSN
            if (page.getPageLSN() == null || page.getPageLSN() <= flushedLsn) {
                disk.flush(pageId, page);
                toBeDeleted.add(pageId);
            }
        }
        for (int pid : toBeDeleted) {
            pageId2Page.remove(pid);
        }
    }
    /**
     * copyPageFrom log in log buffer into disk
     */
    public void flushLog() {
        for (LogRecord log : writeAheadLogs) {
            flushedLsn = Math.max(flushedLsn, log.getLsn());
            disk.flush(log);
        }
        writeAheadLogs.clear();
    }


    public Page getDataPage(int pageId) {
        // if this page already in memory just fetch it, otherwise fetch it from disk
        if (pageId2Page.containsKey(pageId)) {
            return pageId2Page.get(pageId);
        }
        Page pageFromDisk = disk.getPage(pageId);
        pageId2Page.put(pageId, pageFromDisk);
        return pageFromDisk;
    }

    public void addLog(LogRecord logRecord) {
        writeAheadLogs.add(logRecord);
    }


    // used by db crash
    public void clear() {
        pageId2Page.clear();
        writeAheadLogs.clear();
        flushedLsn = 0;
        lsnProducer = new AtomicInteger(disk.getLastLogLsn());
    }

    public int nextLsn() {
        return lsnProducer.incrementAndGet();
    }
}
