package dbengine.transaction;

import dbengine.storage.IDBLock;

public enum LockMode {
    SHARE {
        @Override
        void lock(IDBLock lock) {
            lock.lockInReadMode();
        }

        @Override
        void unlock(IDBLock lock) {
            lock.unlockInReadMode();
        }
    }, EXCLUSIVE {
        @Override
        void lock(IDBLock lock) {
            lock.lockInWriteMode();
        }

        @Override
        void unlock(IDBLock lock) {
            lock.unlockInWriteMode();
        }
    };
    abstract void lock(IDBLock lock);
    abstract void unlock(IDBLock lock);
}
