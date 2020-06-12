package dbengine.storage;

import dbengine.storage.clusterindex.IPrimaryTuple;

public interface IPrimaryIndex extends IUniqueIndex {
    /**
     * primary index should support insert value,
     * it must have primary id and txn id to support MVCC
     * other fields is optional
     *
     * @param pid
     * @param txnId
     * @param otherFields
     * @return
     */
    IPrimaryTuple buildInsertTuple(int pid, int txnId, Comparable... otherFields);

    /**
     * primary index need to build search tuple from secondary index to do a search
     *
     * @param primaryId
     * @return primary record from data
     */
    IPrimaryTuple buildSearchTuple(int primaryId);
}
