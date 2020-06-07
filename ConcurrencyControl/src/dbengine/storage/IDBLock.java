package dbengine.storage;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public interface IDBLock {
    ReadWriteLock rwLock = new ReentrantReadWriteLock();
    AtomicInteger gapLockCnt = new AtomicInteger(0);

    default void lockInReadMode() {rwLock.readLock().lock();}

    default void unlockInReadMode() {rwLock.readLock().unlock();}

    default void lockInWriteMode() {rwLock.writeLock().lock();}

    default void unlockInWriteMode() {rwLock.writeLock().unlock();}

    default void unlockGapLock() {
        gapLockCnt.decrementAndGet();
    }

    default void lockGapLock() {
        gapLockCnt.incrementAndGet();
    }

    GapLock getGapLock();

}
