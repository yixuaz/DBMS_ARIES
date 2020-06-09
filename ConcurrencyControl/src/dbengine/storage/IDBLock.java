package dbengine.storage;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public interface IDBLock {

    ReadWriteLock getRWLock();
    GapLock getGapLock();

}
