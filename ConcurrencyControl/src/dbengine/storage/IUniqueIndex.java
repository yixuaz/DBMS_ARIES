package dbengine.storage;

import dbengine.transaction.IIsolationLevel;
import serverlayer.model.IndexSearchResult;
import serverlayer.model.Predicate;

public interface IUniqueIndex extends IIndex {
    boolean containsKey(ITuple search);
}
