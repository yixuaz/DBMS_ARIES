package dbengine.storage;


import java.util.concurrent.locks.ReadWriteLock;

public interface IDBLock {

    ReadWriteLock getRWLock();
    GapLock getGapLock();

}
