package dbengine.storage.clusterindex;

import dbengine.storage.ITuple;
import dbengine.storage.multipleversion.IDeltaStorageRecordIterator;
import dbengine.storage.multipleversion.IDeltaStorageRecordUpdater;

/**
 * Tuple which is saved in Primary Index should implements IPrimaryTuple
 *
 * @param <T>
 */
public interface IPrimaryTuple<T extends ITuple> extends ITuple<T>, IDeltaStorageRecordIterator {
    /**
     * MVCC , primary record have multiple versions with IDeltaStorageRecordUpdater,
     * use it to build a new IPrimaryTuple return to client
     *
     * @param record
     * @return
     */
    IPrimaryTuple<T> buildOldVersion(IDeltaStorageRecordUpdater record);
}
