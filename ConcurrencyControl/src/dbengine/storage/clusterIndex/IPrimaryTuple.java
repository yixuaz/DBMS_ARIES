package dbengine.storage.clusterIndex;

import dbengine.storage.multipleversion.IDeltaStorageRecordUpdater;
import dbengine.storage.multipleversion.IDeltaStorageRecordIterator;
import dbengine.storage.ITuple;

public interface IPrimaryTuple<T extends ITuple> extends ITuple<T>, IDeltaStorageRecordIterator {
    IPrimaryTuple buildOldVersion(IDeltaStorageRecordUpdater record);
}
