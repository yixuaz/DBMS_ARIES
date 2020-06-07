package dbengine.storage.nonclusterIndex;

import dbengine.storage.ITuple;
import dbengine.storage.GapLock;

import static dbms.SystemCatalog.END_DUMMY_TXN_ID_TAG;

public class NonUniqueIndexTuple implements ITuple<NonUniqueIndexTuple> {
    String name;
    int primaryId;
    NonUniqueIndexTuple next;
    NonUniqueIndexTuple prev;
    int txnId;

    public NonUniqueIndexTuple(ITuple raw) {
        name = (String) raw.getOffsetValue(1);
        primaryId = (Integer) raw.getOffsetValue(0);
    }

    public NonUniqueIndexTuple(String name, int primaryId, int txnId) {
        this.name = name;
        this.primaryId = primaryId;
        this.txnId = txnId;
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
    public GapLock getGapLock() {
        if (gapLockCnt.get() > 0) {
            return new GapLock( prev, this.txnId == END_DUMMY_TXN_ID_TAG ? null : this);
        }
        return null;
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
    public GapLock getPairGapLock() {
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
        return  "[S_I]name='" + name + '\'' +
                ", primaryId=" + primaryId;
    }
}
