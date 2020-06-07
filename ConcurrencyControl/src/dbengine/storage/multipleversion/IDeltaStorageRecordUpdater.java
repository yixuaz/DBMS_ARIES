package dbengine.storage.multipleversion;

import dbengine.storage.clusterIndex.IPrimaryTuple;

public interface IDeltaStorageRecordUpdater {
    boolean update(IPrimaryTuple clone);
}
