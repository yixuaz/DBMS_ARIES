package dbengine.storage;

import serverlayer.model.IndexSearchResult;
import serverlayer.model.Predicate;

/**
 * each(primary/ unique/ non-unique) index of a table should implement this interface
 */
public interface IIndex {
    /**
     * use search tuple to find the first tuple from index which is larger equal than the search tuple
     *
     * @param searchKey
     * @return the first tuple which is larger equal than search tuple, if not found, return end dummy
     */
    ITuple findTuple(ITuple searchKey);

    /**
     * @return the first small tuple in index, if data set is empty, return end dummy
     */
    ITuple firstTuple();

    /**
     * insert a tuple in the index
     * @param tuple
     * @return the real inserted tuple (eg. have primary id assigned)
     */
    ITuple insert(ITuple tuple);

    /**
     * @return the end dummy of this index (represent supremum)
     */
    ITuple endDummy();

    /**
     * this method is used to do the lock improvement
     * if this search from a unique index and is equal value search
     * and success match the predicate. next key could downgrade to record key
     *
     * @param predicate
     * @return IndexSearchResult (include targetIsTreeSearchWithEqualExp, lastTupleIsInvalid, and the searched tuple)
     */
    IndexSearchResult firstTreeSearch(Predicate predicate);
}
