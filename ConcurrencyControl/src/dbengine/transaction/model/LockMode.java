package dbengine.transaction.model;

import dbengine.storage.IDbTupleLock;

public enum LockMode {
    SHARE {
        @Override
        public void lock(IDbTupleLock lock) {
            lock.getRecordLock().readLock().lock();
        }

        @Override
        public void unlock(IDbTupleLock lock) {
            lock.getRecordLock().readLock().unlock();
        }
    }, EXCLUSIVE {
        @Override
        public void lock(IDbTupleLock lock) {
            lock.getRecordLock().writeLock().lock();
        }

        @Override
        public void unlock(IDbTupleLock lock) {
            lock.getRecordLock().writeLock().unlock();
        }
    }, INSERT_INTENTION {
        @Override
        public void lock(IDbTupleLock lock) {
            lock.getGapLock().insertIntention();
        }

        @Override
        public void unlock(IDbTupleLock lock) {
            lock.getGapLock().refresh();
        }
    }, GAP_LOCK {
        @Override
        public void lock(IDbTupleLock lock) {
            lock.getGapLock().lock();
        }

        @Override
        public void unlock(IDbTupleLock lock) {
            lock.getGapLock().unlock();
        }
    };

    public abstract void lock(IDbTupleLock lock);

    public abstract void unlock(IDbTupleLock lock);
}
