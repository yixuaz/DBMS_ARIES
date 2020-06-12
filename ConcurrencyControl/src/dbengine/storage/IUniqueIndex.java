package dbengine.storage;

public interface IUniqueIndex extends IIndex {
    /**
     * if a index is unique index, we need to implement containsKey to check duplication job
     *
     * @param search
     * @return success or not
     */
    boolean containsKey(ITuple search);
}
