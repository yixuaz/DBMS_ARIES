package dbengine.storage.tables;

import dbengine.storage.clusterIndex.PrimaryIndex;
import dbengine.storage.nonclusterIndex.NonUniqueIndex;

public interface ITable {
    PrimaryIndex getClusterIndex();
    NonUniqueIndex getSecondaryIndex(int offset);
    int columns();
}
