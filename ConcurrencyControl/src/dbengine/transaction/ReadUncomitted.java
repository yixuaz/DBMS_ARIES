package dbengine.transaction;

import dbengine.storage.ITuple;
import dbengine.storage.LockType;
import dbms.SystemCatalog;

import java.util.ArrayList;
import java.util.List;

public class ReadUncomitted implements IIsolationLevel {
    List<HoldLock> holdLocks = new ArrayList<>();
//    @Override
//    public void insert(IIndex table, ITuple tuple) {
//        GapLock lock = table.findGapLock(tuple);
//        lock.lockInWriteMode();
//        ITuple ret = table.insert(tuple);
//        lock.unlockInWriteMode();
//        addLock(LockMode.EXCLUSIVE, ret);
//    }


    @Override
    public ITuple lockIfVisible(ITuple ret, LockMode lockMode, TxnReadView readView, LockStrategy strategy) {
        if (ret.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
            addLock(lockMode, ret, LockType.RECORD_LOCK);
        }
        return ret;
    }


    @Override
    public void unlockIfPossible(ITuple ret, ITuple backup, LockMode lockMode, boolean isTreeSearchLastNotMatchCondition) {
        if (ret.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
            removeLock(lockMode, ret, LockType.RECORD_LOCK);
        }
        removeLock(lockMode, backup, LockType.RECORD_LOCK);
    }

    @Override
    public TxnReadView getTxnReadView() {
        return null;
    }

    @Override
    public List<HoldLock> getHoldLocks() {
        return holdLocks;
    }
}
