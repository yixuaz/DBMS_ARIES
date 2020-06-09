package dbengine.transaction;


import dbengine.storage.LockType;
import dbengine.storage.multipleversion.IDeltaStorageRecordIterator;
import dbengine.storage.multipleversion.IDeltaStorageRecordUpdater;
import dbengine.storage.ITuple;
import dbengine.storage.clusterIndex.IPrimaryTuple;
import dbms.DBEngineGlobalEnvironment;
import dbms.SystemCatalog;

import java.util.ArrayList;
import java.util.List;

public class ReadComitted implements IIsolationLevel {
    List<HoldLock> holdLocks = new ArrayList<>();
    @Override
    public ITuple lockIfVisible(ITuple ret, LockMode lockMode, TxnReadView readView,  LockStrategy strategy) {
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
            if (lockMode != LockMode.INSERT_INTENTION && ret.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
                addLock(lockMode, ret);
            }
            return ret;
        }
    }

    @Override
    public void unlockIfPossible(ITuple ret, ITuple backup, LockMode lockMode, boolean isTreeSearchLastNotMatchCondition) {
        if (ret.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
            removeLock(lockMode, ret);
        }
        if (backup != null && backup.getTxnId() != SystemCatalog.END_DUMMY_TXN_ID_TAG) {
            removeLock(lockMode, backup);
        }
    }

    @Override
    public TxnReadView getTxnReadView() {
        int txnId = SystemCatalog.getTxnId(Thread.currentThread().getId());
        return new TxnReadView(txnId);
    }

    @Override
    public List<HoldLock> getHoldLocks() {
        return holdLocks;
    }


//    @Override
//    public void insert(IIndex table, ITuple tuple) {
//        GapLock lock = table.findGapLock(tuple);
//        lock.lockInWriteMode();
//        ITuple ret = table.insert(tuple);
//        lock.unlockInWriteMode();
//        addLock(LockMode.EXCLUSIVE, ret);
//    }



}
