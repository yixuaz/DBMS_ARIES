package dbengine.transaction;


import dbengine.storage.ITuple;
import dbengine.transaction.model.HoldLock;
import dbengine.transaction.model.LockMode;
import dbengine.transaction.model.LockStrategy;
import dbengine.transaction.model.TxnReadView;

import java.util.List;

public class Serializable extends IsolationLevelCommon {

    @Override
    public ITuple lockIfVisible(ITuple toBeLocked, LockMode lockMode, TxnReadView readView, LockStrategy strategy) {
        if (lockMode == null) {
            lockMode = LockMode.SHARE;
        }
        return acquireLockImprovementInRR(toBeLocked, lockMode, strategy);
    }

    @Override
    public void unlockIfPossible(ITuple primaryLockedTuple, ITuple secondaryLockedTuple,
                                 LockMode lockMode, boolean isTreeSearchLastNotMatchCondition) {
        if (lockMode == null) {
            lockMode = LockMode.SHARE;
        }
        releaseLockImprovementInRR(primaryLockedTuple, secondaryLockedTuple, lockMode, isTreeSearchLastNotMatchCondition);
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
