package dbengine.storage.tables;

import dbengine.storage.IIndex;
import dbengine.storage.IPrimaryIndex;

import java.util.List;

/**
 * table class should implement this interface
 */
public interface ITable {
    /**
     * table should have ClusterIndex
     *
     * @return IPrimaryIndex
     */
    IPrimaryIndex getClusterIndex();

    /**
     * return SecondaryIndex according to offset (it not support compound index yet)
     *
     * @return IIndex
     */
    IIndex getSecondaryIndex(int offset);

    /**
     * get all secondary indexes
     *
     * @return
     */
    List<IIndex> secondaryIndexes();
}
