package dbengine.storage.multipleversion;

import dbengine.storage.clusterindex.IPrimaryTuple;

/**
 * enable use DeltaStorageRecord update functions to update a cloned IPrimaryTuple
 * if changes happen return true, else false (used by updated X rows)
 */
public interface IDeltaStorageRecordUpdater {
    boolean update(IPrimaryTuple clone);
}
