package dbengine.transaction;

import dbengine.storage.ITuple;
import dbengine.transaction.model.LockMode;
import dbengine.transaction.model.LockStrategy;
import dbengine.transaction.model.TxnReadView;
import dbms.SystemCatalog;

public class ReadUncomitted extends IsolationLevelCommon {

    @Override
    public ITuple lockIfVisible(ITuple ret, LockMode lockMode, TxnReadView readView, LockStrategy strategy) {
        if (lockMode != LockMode.INSERT_INTENTION && ret.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
            addLock(lockMode, ret);
        }
        return ret;
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

}
