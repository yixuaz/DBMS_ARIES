package dbengine.storage.tables;

import dbengine.storage.IIndex;
import dbengine.storage.clusterindex.PrimaryIndex;
import dbengine.storage.nonclusterindex.NonUniqueIndex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MyTable implements ITable {
    private final PrimaryIndex clusterIndexTable = new PrimaryIndex();
    private final NonUniqueIndex nameIndexTable = new NonUniqueIndex();
    private final List<IIndex> secondaryIndexes = new ArrayList<>(Arrays.asList(nameIndexTable));
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
