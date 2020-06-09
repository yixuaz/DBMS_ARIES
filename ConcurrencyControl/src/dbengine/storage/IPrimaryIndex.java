package dbengine.storage;

import dbengine.storage.clusterIndex.IPrimaryTuple;

public interface IPrimaryIndex extends IUniqueIndex {
    IPrimaryTuple buildInsertTuple(int pid, int txnId, Comparable... otherFields);
}
