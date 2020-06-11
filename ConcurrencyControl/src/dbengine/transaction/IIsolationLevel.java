package dbengine.transaction;

import dbengine.storage.IDBLock;
import dbengine.storage.ITuple;
import dbengine.storage.LockType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

public interface IIsolationLevel {
    ITuple lockIfVisible(ITuple ret, LockMode lockMode, TxnReadView readView, LockStrategy strategy);

    void unlockIfPossible(ITuple ret, ITuple backup, LockMode lockMode, boolean isTreeSearchLastNotMatchCondition);

    default void commit() {
        for (HoldLock lock : getHoldLocks()) {
            lock.mode.unlock(lock.lock);
        }
        getHoldLocks().clear();
    }

    default void addLock(LockMode mode, IDBLock lock) {
        if (mode != null) {
            mode.lock(lock);
            getHoldLocks().add(new HoldLock(lock, mode));
        }
    }
    default void removeLock(LockMode mode, IDBLock lock) {
        if (mode != null && lock != null) {
            mode.unlock(lock);
            if (!getHoldLocks().remove(new HoldLock(lock, mode))) {
                throw new IllegalStateException();
            }
        }
    }

    TxnReadView getTxnReadView();
    List<HoldLock> getHoldLocks();

    default void printLockInfo() {
        if (getHoldLocks().isEmpty()) {
            System.out.println("no locks");
        }
        for (HoldLock lock : getHoldLocks()) {
            System.out.println(lock);
        }
    }


}
