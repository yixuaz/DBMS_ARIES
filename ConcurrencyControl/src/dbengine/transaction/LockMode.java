package dbengine.transaction;

import dbengine.storage.IDBLock;
import dbms.SystemCatalog;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public enum LockMode {
    SHARE {
        @Override
        public void lock(IDBLock lock) {
            lock.getRWLock().readLock().lock();
        }

        @Override
        public void unlock(IDBLock lock) {
            lock.getRWLock().readLock().unlock();
        }
    }, EXCLUSIVE {
        @Override
        public void lock(IDBLock lock) {
            lock.getRWLock().writeLock().lock();
        }

        @Override
        public void unlock(IDBLock lock) {
            lock.getRWLock().writeLock().unlock();
        }
    }, INSERT_INTENTION {
        @Override
        public void lock(IDBLock lock) {
            lock.getGapLock().insertIntention();
        }

        @Override
        public void unlock(IDBLock lock) {
            lock.getGapLock().refresh();
        }
    }, GAP_LOCK {
        @Override
        public void lock(IDBLock lock) {
            lock.getGapLock().lock();
        }

        @Override
        public void unlock(IDBLock lock) {
            lock.getGapLock().unlock();
        }
    };

    public abstract void lock(IDBLock lock);

    public abstract void unlock(IDBLock lock);
}
