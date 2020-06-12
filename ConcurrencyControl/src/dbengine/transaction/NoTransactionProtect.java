package dbengine.transaction;

import dbengine.storage.ITuple;
import dbengine.transaction.model.LockMode;
import dbengine.transaction.model.LockStrategy;
import dbengine.transaction.model.TxnReadView;

public class NoTransactionProtect extends IsolationLevelCommon {
    @Override
    public ITuple lockIfVisible(ITuple ret, LockMode lockMode, TxnReadView readView, LockStrategy strategy) {
        return ret;
    }

    @Override
    public void unlockIfPossible(ITuple primaryLockedTuple, ITuple secondaryLockedTuple,
                                 LockMode lockMode, boolean isTreeSearchLastNotMatchCondition) {
    }
}
