package dbengine.storage.tables;

import dbengine.storage.clusterIndex.PrimaryIndex;
import dbengine.storage.nonclusterIndex.NonUniqueIndex;

public class MyTable implements ITable {
    PrimaryIndex clusterIndexTable = new PrimaryIndex();
    NonUniqueIndex nameIndexTable = new NonUniqueIndex();

    public PrimaryIndex getClusterIndex() {
        return clusterIndexTable;
    }

    @Override
    public NonUniqueIndex getSecondaryIndex(int offset) {
        if (offset == 1) return nameIndexTable;
        return null;
    }

    @Override
    public int columns() {
        return 3;
    }
}
