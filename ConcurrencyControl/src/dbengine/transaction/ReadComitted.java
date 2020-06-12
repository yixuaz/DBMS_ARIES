package dbengine.transaction;


import dbengine.storage.ITuple;
import dbengine.transaction.model.LockMode;
import dbengine.transaction.model.LockStrategy;
import dbengine.transaction.model.TxnReadView;
import dbms.SystemCatalog;

public class ReadComitted extends IsolationLevelCommon {

    @Override
    public ITuple lockIfVisible(ITuple lockedTuple, LockMode lockMode, TxnReadView readView, LockStrategy strategy) {
        if (lockMode == null) {
            return findVisibleTuple(lockedTuple, readView);
        } else {
            if (lockMode != LockMode.INSERT_INTENTION
                    && lockedTuple.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
                addLock(lockMode, lockedTuple);
            }
            return lockedTuple;
        }
    }

    @Override
    public void unlockIfPossible(ITuple primaryLockedTuple, ITuple secondaryLockedTuple, LockMode lockMode, boolean isTreeSearchLastNotMatchCondition) {
        if (primaryLockedTuple.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
            removeLock(lockMode, primaryLockedTuple);
        }
        if (secondaryLockedTuple != null && secondaryLockedTuple.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
            removeLock(lockMode, secondaryLockedTuple);
        }
    }

    @Override
    public TxnReadView getTxnReadView() {
        int txnId = SystemCatalog.getTxnId(Thread.currentThread().getId());
        return new TxnReadView(txnId);
    }
}
