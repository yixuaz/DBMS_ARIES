package dbengine.transaction.model;

import dbengine.storage.IDbTupleLock;

import java.util.Objects;

public class HoldLock {
    private IDbTupleLock lock;
    private LockMode mode;

    public HoldLock(IDbTupleLock lock, LockMode mode) {
        this.lock = lock;
        this.mode = mode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HoldLock)) return false;
        HoldLock holdLock = (HoldLock) o;
        return Objects.equals(lock, holdLock.lock) &&
                mode == holdLock.mode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lock, mode);
    }

    @Override
    public String toString() {
        if (mode == LockMode.INSERT_INTENTION) {
            return "INSERT_" + lock.getGapLock().toString() + "," + lock.toString();
        } else if (mode == LockMode.GAP_LOCK) {
            return lock.getGapLock().toString() + "," + lock.toString();
        } else {
            return mode + "_LOCK" + "," + lock.toString();
        }
    }


    public void unlock() {
        mode.unlock(lock);
    }
}
