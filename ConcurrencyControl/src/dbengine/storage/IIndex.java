package dbengine.storage;

import dbengine.transaction.LockMode;
import serverlayer.model.IndexSearchResult;
import serverlayer.model.Predicate;

public interface IIndex {
    ITuple findTuple(ITuple searchKey);
    ITuple firstTuple();
    ITuple insert(ITuple tuple);
    ITuple endDummy();
    IndexSearchResult firstSearch(Predicate predicate);
}
