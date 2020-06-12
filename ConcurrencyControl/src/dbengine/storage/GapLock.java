package dbengine.storage;

import dbengine.storage.clusterindex.PrimaryTuple;
import dbengine.storage.nonclusterindex.NonUniqueIndexTuple;
import dbms.SystemCatalog;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GapLock {
    private static final Comparable MINIMUM = other -> -1;
    private static final Comparable MAXIMUM = other -> 1;

    private Comparable lowBound;

    private final Comparable highBound;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ITuple parent;

    public GapLock(ITuple lowBound, ITuple highBound) {
        this.lowBound = (lowBound == null) ?  MINIMUM : lowBound;
        this.highBound = (highBound.getTxnId() == SystemCatalog.END_DUMMY_TXN_ID_TAG) ?  MAXIMUM : highBound;
        this.parent = highBound;
    }

    public boolean inGap(Comparable input) {
        return input.compareTo(lowBound) > 0 && input.compareTo(highBound) < 0;
    }


    @Override
    public String toString() {
        return "GAP_LOCK(" + lowBound()
                + "," + highBound() + ")";
    }
    private String lowBound() {
        if (lowBound instanceof NonUniqueIndexTuple) {return ((NonUniqueIndexTuple) lowBound).getOffsetValue(1).toString()
                + ":" + ((ITuple) lowBound).getOffsetValue(0).toString();}
        else if (lowBound instanceof PrimaryTuple) return ((ITuple) lowBound).getOffsetValue(0).toString();
        else return "-inf";
    }
    private String highBound() {
        if (highBound == MAXIMUM) return "inf";
        else if (highBound instanceof NonUniqueIndexTuple){
            return ((NonUniqueIndexTuple) highBound).getOffsetValue(1).toString() + ":" + ((ITuple) highBound).getOffsetValue(0).toString();
        }
        else if (highBound instanceof PrimaryTuple) return ((ITuple) highBound).getOffsetValue(0).toString();
        else throw new IllegalStateException("");
    }

    public void insertIntention() {
        rwLock.writeLock().lock();
        rwLock.writeLock().unlock();
    }

    public void refresh() {
        this.lowBound = (parent.prev() == null) ?  MINIMUM : parent.prev();
    }

    public void lock() {
        rwLock.readLock().lock();
    }
    public void unlock() {
        rwLock.readLock().unlock();
    }
}
