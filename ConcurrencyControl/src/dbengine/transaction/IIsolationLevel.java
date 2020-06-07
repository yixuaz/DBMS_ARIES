package dbengine.transaction;

import dbengine.storage.IDBLock;
import dbengine.storage.ITuple;
import dbengine.storage.LockType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

public interface IIsolationLevel {
    ITuple lockIfVisible(ITuple ret, LockMode lockMode, TxnReadView readView, LockStrategy strategy);
//    default ITuple lockIfVisible(ITuple ret, LockMode lockMode, TxnReadView readView) {
//        // we don't care if it is tree search  in RU AND RC
//        return lockIfVisible(ret, lockMode, readView, null);
//    }

    void unlockIfPossible(ITuple ret, ITuple backup, LockMode lockMode, boolean isTreeSearchLastNotMatchCondition);


    // void insert(IIndex table, ITuple tuple);

//    ITuple select(IIndex table, ITuple searchKey, LockMode mode);
//
//    ITuple next(ITuple prevTuple, LockMode mode);

    default void commit() {
        for (HoldLock lock : getHoldLocks()) {
            if (lock.type == LockType.RECORD_LOCK) {
                if (lock.mode == LockMode.SHARE) {
                    lock.lock.unlockInReadMode();
                } else {
                    lock.lock.unlockInWriteMode();
                }
            } else {
                lock.lock.unlockGapLock();
            }
        }
        getHoldLocks().clear();
    }

    default void addLock(LockMode mode, IDBLock lock, LockType type) {
        if (mode != null) {
            if (type == LockType.RECORD_LOCK) {
                mode.lock(lock);
            } else {
                lock.lockGapLock();
            }
            getHoldLocks().add(new HoldLock(lock, mode, type));
        }
    }
    default void removeLock(LockMode mode, IDBLock lock, LockType type) {
        if (mode != null && lock != null) {
            if (type == LockType.RECORD_LOCK) {
                mode.unlock(lock);
            } else {
                lock.unlockGapLock();
            }
            assert getHoldLocks().remove(new HoldLock(lock, mode, type));
        }
    }

    TxnReadView getTxnReadView();
    List<HoldLock> getHoldLocks();

    default void printLockInfo() {
        for (HoldLock lock : getHoldLocks()) {
            System.out.println(lock);
        }
    }


}
