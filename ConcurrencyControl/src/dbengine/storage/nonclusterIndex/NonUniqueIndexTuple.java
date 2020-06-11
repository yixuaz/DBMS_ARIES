package dbengine.storage.nonclusterIndex;

import dbengine.storage.GapLock;
import dbengine.storage.ITuple;
import util.MyReadWriteLock;

import java.util.concurrent.locks.ReadWriteLock;

public class NonUniqueIndexTuple implements ITuple<NonUniqueIndexTuple> {
    String name;
    int primaryId;
    NonUniqueIndexTuple next;
    NonUniqueIndexTuple prev;
    int txnId;

    final ReadWriteLock rwLock = new MyReadWriteLock();
    final GapLock gapLock;
    public NonUniqueIndexTuple(ITuple raw) {
        this((String) raw.getOffsetValue(1), (Integer) raw.getOffsetValue(0), raw.getTxnId());
    }

    public NonUniqueIndexTuple(String name, int primaryId, int txnId) {
        this.name = name;
        this.primaryId = primaryId;
        this.txnId = txnId;
        gapLock = new GapLock(prev, this);
    }

    @Override
    public boolean isPrimary() {
        return false;
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    @Override
    public boolean haveOffsetValue(int offset) {
        return offset >= 0 && offset <= 1;
    }

    @Override
    public Comparable getOffsetValue(int offset) {
        if (offset == 0) return primaryId;
        else if (offset == 1) return name;
        return null;
    }

    @Override
    public void setOffsetValue(int offset, Comparable val, int txnId) {
        // no need in this project
    }

    @Override
    public ReadWriteLock getRWLock() {
        return rwLock;
    }


    @Override
    public GapLock getGapLock() {
        return gapLock;
    }

    @Override
    public ITuple prev() {
        return prev;
    }

    @Override
    public ITuple next() {
        return next;
    }

    @Override
    public String getOffsetName(int columns) {
        if (columns == 0) return "id";
        else if (columns == 1) return "name";
        return null;
    }


    @Override
    public int getTxnId() {
        return txnId;
    }


    @Override
    public int compareTo(NonUniqueIndexTuple o) {
        int res = name.compareTo(o.name);
        if (res == 0) {
            return Integer.compare(primaryId, o.primaryId);
        }
        return res;
    }

    @Override
    public String toString() {
        return  "[SEC_IDX]name='" + name + '\'' +
                ", primaryId=" + primaryId;
    }
}
