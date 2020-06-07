package dbengine.transaction;

import dbengine.storage.IDBLock;
import dbengine.storage.LockType;

import java.util.Objects;

public class HoldLock {
    IDBLock lock;
    LockMode mode;
    LockType type;

    public HoldLock(IDBLock lock, LockMode mode, LockType type) {
        this.lock = lock;
        this.mode = mode;
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HoldLock)) return false;
        HoldLock holdLock = (HoldLock) o;
        return Objects.equals(lock, holdLock.lock) &&
                mode == holdLock.mode &&
                type == holdLock.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lock, mode, type);
    }

    @Override
    public String toString() {
        if (type == LockType.GAP_LOCK) {
            return lock.getGapLock().toString() + "," + lock.toString();
        } else {
            return lock.toString() + "," + mode;
        }
    }


}
