package dbengine.transaction;

import dbengine.storage.IDbTupleLock;
import dbengine.storage.ITuple;
import dbengine.transaction.model.HoldLock;
import dbengine.transaction.model.LockMode;
import dbengine.transaction.model.LockStrategy;
import dbengine.transaction.model.TxnReadView;

import java.util.List;

public interface IIsolationLevel {
    /**
     * if a recode is visible we need to add next-key lock on it depend on different IsolationLevelType
     *
     * @param lockedTuple
     * @param lockMode    share/ exclusive / gap / insert_intention
     * @param readView    used to check this tuple is visible
     * @param strategy    used to do some lock improvement
     * @return the visible tuple
     */
    ITuple lockIfVisible(ITuple lockedTuple, LockMode lockMode, TxnReadView readView, LockStrategy strategy);

    /**
     * if this tuple is not meet predicate we could release lock early
     *
     * @param primaryLockedTuple
     * @param secondaryLockedTuple
     * @param lockMode
     * @param isTreeSearchLastNotMatchCondition
     */
    void unlockIfPossible(ITuple primaryLockedTuple, ITuple secondaryLockedTuple,
                          LockMode lockMode, boolean isTreeSearchLastNotMatchCondition);

    /**
     * @return current transaction read view used to decide a tuple is visible or not
     */
    TxnReadView getTxnReadView();

    /**
     * @return all locks which not released yet
     */
    List<HoldLock> getHoldLocks();


    default void commit() {
        for (HoldLock lock : getHoldLocks()) {
            lock.unlock();
        }
        getHoldLocks().clear();
    }

    default void addLock(LockMode mode, IDbTupleLock lock) {
        if (mode != null) {
            mode.lock(lock);
            getHoldLocks().add(new HoldLock(lock, mode));
        }
    }

    default void removeLock(LockMode mode, IDbTupleLock lock) {
        if (mode != null && lock != null) {
            mode.unlock(lock);
            if (!getHoldLocks().remove(new HoldLock(lock, mode))) {
                throw new IllegalStateException();
            }
        }
    }

    default void printLockInfo() {
        if (getHoldLocks().isEmpty()) {
            System.out.println("no locks");
        }
        for (HoldLock lock : getHoldLocks()) {
            System.out.println(lock);
        }
    }


}
