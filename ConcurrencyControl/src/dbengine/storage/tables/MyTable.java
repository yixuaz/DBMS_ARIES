package dbengine.storage.tables;

import dbengine.storage.IIndex;
import dbengine.storage.clusterIndex.PrimaryIndex;
import dbengine.storage.nonclusterIndex.NonUniqueIndex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MyTable implements ITable {
    final PrimaryIndex clusterIndexTable = new PrimaryIndex();
    final NonUniqueIndex nameIndexTable = new NonUniqueIndex();
    final List<IIndex> secondaryIndexes = new ArrayList<>(Arrays.asList(nameIndexTable));
    public PrimaryIndex getClusterIndex() {
        return clusterIndexTable;
    }

    @Override
    public NonUniqueIndex getSecondaryIndex(int offset) {
        if (offset == 1) return nameIndexTable;
        return null;
    }

    @Override
    public List<IIndex> secondaryIndexes() {
        return Collections.unmodifiableList(secondaryIndexes);
    }
}
