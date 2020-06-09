package dbengine.transaction;


import dbengine.storage.ITuple;
import dbengine.storage.clusterIndex.IPrimaryTuple;
import dbengine.storage.multipleversion.IDeltaStorageRecordIterator;
import dbengine.storage.multipleversion.IDeltaStorageRecordUpdater;
import dbms.SystemCatalog;

import java.util.ArrayList;
import java.util.List;

public class RepeatableRead implements IIsolationLevel {
    TxnReadView readView = null;
    List<HoldLock> holdLocks = new ArrayList<>();


    @Override
    public ITuple lockIfVisible(ITuple ret, LockMode lockMode, TxnReadView readView, LockStrategy strategy) {
        if (lockMode == null) {
            if (ret.isPrimary()) {
                IDeltaStorageRecordIterator pointer = (IDeltaStorageRecordIterator) ret;
                while (pointer != null && !readView.isVisble(pointer.getTxnId())) {
                    pointer = pointer.getPrevVersionRecord();
                }
                if (pointer == null) {
                    return null;
                }
                if (pointer instanceof IDeltaStorageRecordUpdater) {
                    return ((IPrimaryTuple) ret).buildOldVersion((IDeltaStorageRecordUpdater) pointer);
                } else {
                    return ret;
                }
            } else {
                return readView.isVisble(ret.getTxnId()) ? ret : null;
            }
        } else if (lockMode == LockMode.INSERT_INTENTION) {
            addLock(lockMode, ret);
            return ret;
        } else {
            if (!strategy.couldOptimizeToRecordLockOnly) {
                addLock(LockMode.GAP_LOCK, ret);
            }
            if (ret.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
                addLock(lockMode, ret);
            }

            return ret;
        }
    }

    @Override
    public void unlockIfPossible(ITuple primary, ITuple secondary,
                                 LockMode lockMode, boolean isTreeSearchLastNotMatchCondition) {
        if (isTreeSearchLastNotMatchCondition) {
            if (secondary != null) {
                removeLock(lockMode, secondary);
            } else if (primary.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
                removeLock(lockMode, primary);
            }
        }
    }

    @Override
    public TxnReadView getTxnReadView() {
        if (readView == null) {
            int txnId = SystemCatalog.getTxnId(Thread.currentThread().getId());
            readView = new TxnReadView(txnId);
        }
        return readView;
    }

    @Override
    public List<HoldLock> getHoldLocks() {
        return holdLocks;
    }
}
