package dbengine.transaction;

import dbengine.storage.ITuple;
import dbms.SystemCatalog;

import java.util.ArrayList;
import java.util.List;

public class NoTransactionProtect implements IIsolationLevel {
    List<HoldLock> holdLocks = new ArrayList<>();


    @Override
    public ITuple lockIfVisible(ITuple ret, LockMode lockMode, TxnReadView readView, LockStrategy strategy) {
        return ret;
    }


    @Override
    public void unlockIfPossible(ITuple ret, ITuple backup, LockMode lockMode, boolean isTreeSearchLastNotMatchCondition) {

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
