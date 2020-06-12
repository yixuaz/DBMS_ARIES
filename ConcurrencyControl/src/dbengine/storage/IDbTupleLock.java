package dbengine.storage;


import java.util.concurrent.locks.ReadWriteLock;

/**
 * IDbTupleLock is same as next-key lock in mysql
 */
public interface IDbTupleLock {
    ReadWriteLock getRecordLock();
    GapLock getGapLock();
}
