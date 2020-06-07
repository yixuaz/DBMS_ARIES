package dbengine.storage;

import dbengine.storage.multipleversion.IMultipleVersion;

public interface ITuple<T extends ITuple> extends IDBLock, IMultipleVersion, Comparable<T> {

    boolean isPrimary();

    boolean isUnique();

    String toString();

    boolean haveOffsetValue(int offset);

    Comparable getOffsetValue(int offset) ;

    void setOffsetValue(int offset, Comparable val, int txnId) ;

    ITuple prev();

    ITuple next();

    GapLock getPairGapLock();

}
