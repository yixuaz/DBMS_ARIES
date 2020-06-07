package dbengine.transaction;


import dbengine.storage.ITuple;
import dbengine.storage.LockType;
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
        } else {
            // 如果是唯一索引，等值查询（第一次树搜索），如果满足条件上行锁，如果不满足条件上间隙锁，且无下一个
            if (strategy.isTreeSearch) {
                if (strategy.meetCondition) {
                    if (ret.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
                        addLock(lockMode, ret, LockType.RECORD_LOCK);
                    }
                    if (!ret.isUnique()) {
                        addLock(lockMode, ret, LockType.GAP_LOCK);
                    }
                } else {
                    addLock(lockMode, ret, LockType.GAP_LOCK);
                    return SystemCatalog.INVALID_TUPLE;
                }
            } else {
                addLock(lockMode, ret, LockType.GAP_LOCK);
                if (ret.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
                    addLock(lockMode, ret, LockType.RECORD_LOCK);
                }
            }
            return ret;
        }
    }

    @Override
    public void unlockIfPossible(ITuple primary, ITuple secondary,
                                 LockMode lockMode, boolean isTreeSearchLastNotMatchCondition) {
        if (isTreeSearchLastNotMatchCondition) {
            if (secondary != null) {
                removeLock(lockMode, secondary, LockType.RECORD_LOCK);
            } else {
                removeLock(lockMode, primary, LockType.RECORD_LOCK);
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
