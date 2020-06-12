package dbengine.transaction;

import dbengine.storage.ITuple;
import dbengine.storage.clusterindex.IPrimaryTuple;
import dbengine.storage.multipleversion.IDeltaStorageRecordIterator;
import dbengine.storage.multipleversion.IDeltaStorageRecordUpdater;
import dbengine.transaction.model.HoldLock;
import dbengine.transaction.model.LockMode;
import dbengine.transaction.model.LockStrategy;
import dbengine.transaction.model.TxnReadView;
import dbms.SystemCatalog;

import java.util.ArrayList;
import java.util.List;

public abstract class IsolationLevelCommon  implements IIsolationLevel{
    protected TxnReadView readView = null;
    protected List<HoldLock> holdLocks = new ArrayList<>();

    public IsolationLevelCommon() {
    }

    public IsolationLevelCommon(TxnReadView readView) {
        this.readView = readView;
    }

    protected ITuple findVisibleTuple(ITuple toBeLocked, TxnReadView readView) {
        if (toBeLocked.isPrimary()) {
            IDeltaStorageRecordIterator pointer = (IDeltaStorageRecordIterator) toBeLocked;
            while (pointer != null && !readView.isVisble(pointer.getTxnId())) {
                pointer = pointer.getPrevVersionRecord();
            }
            if (pointer == null) {
                return null;
            } else if (pointer instanceof IDeltaStorageRecordUpdater) {
                return ((IPrimaryTuple) toBeLocked).buildOldVersion((IDeltaStorageRecordUpdater) pointer);
            } else {
                return toBeLocked;
            }
        } else {
            return readView.isVisble(toBeLocked.getTxnId()) ? toBeLocked : null;
        }
    }

    protected void releaseLockImprovementInRR(ITuple primaryLockedTuple, ITuple secondaryLockedTuple,
                                            LockMode lockMode, boolean isTreeSearchLastNotMatchCondition) {
        if (isTreeSearchLastNotMatchCondition) {
            if (secondaryLockedTuple != null) {
                removeLock(lockMode, secondaryLockedTuple);
            } else if (primaryLockedTuple.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
                removeLock(lockMode, primaryLockedTuple);
            }
        }
    }
    protected ITuple acquireLockImprovementInRR(ITuple toBeLocked, LockMode lockMode, LockStrategy strategy) {
        if (lockMode == LockMode.INSERT_INTENTION) {
            addLock(lockMode, toBeLocked);
            return toBeLocked;
        }
        if (!strategy.couldOptimizeToRecordLockOnly) {
            addLock(LockMode.GAP_LOCK, toBeLocked);
        }
        if (toBeLocked.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
            addLock(lockMode, toBeLocked);
        }
        return toBeLocked;
    }

    @Override
    public List<HoldLock> getHoldLocks() {
        return holdLocks;
    }

    @Override
    public TxnReadView getTxnReadView() {
        return readView;
    }


}
