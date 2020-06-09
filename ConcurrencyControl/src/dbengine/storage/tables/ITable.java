package dbengine.storage.tables;

import dbengine.storage.IIndex;
import dbengine.storage.clusterIndex.PrimaryIndex;
import dbengine.storage.nonclusterIndex.NonUniqueIndex;

import java.util.List;

public interface ITable {
    PrimaryIndex getClusterIndex();
    NonUniqueIndex getSecondaryIndex(int offset);
    int columns();
    List<IIndex> secondaryIndexes();
}
