package dbengine.storage.clusterindex;

import dbengine.storage.Expression;
import dbengine.storage.IPrimaryIndex;
import dbengine.storage.ITuple;
import dbms.SystemCatalog;
import serverlayer.model.IndexSearchResult;
import serverlayer.model.Predicate;

import java.util.TreeSet;

import static dbms.SystemCatalog.END_DUMMY_TXN_ID_TAG;

/**
 * this class is used by storing db data
 * the data is saved in a red-black tree(2-3 B-tree), with link to prev/next node
 * the last there is a end dummy to represent supremum
 */
public class PrimaryIndex implements IPrimaryIndex {
    private static final PrimaryTuple endDummy = new PrimaryTuple(0, null, 0,
            null, null, END_DUMMY_TXN_ID_TAG);
    private final TreeSet<PrimaryTuple> primaryId2Tuples = new TreeSet<>();

    public PrimaryIndex() {
        // import initial data
        primaryId2Tuples.add(new PrimaryTuple(1, "aaa", 100, null, null, 0));
        primaryId2Tuples.add(new PrimaryTuple(2, "bbb", 200, null, null, 0));
        primaryId2Tuples.add(new PrimaryTuple(3, "bbb", 300, null, null, 0));
        primaryId2Tuples.add(new PrimaryTuple(7, "ccc", 200, null, null, 0));
        PrimaryTuple pre = null;
        // setup linked list
        for (PrimaryTuple cur : primaryId2Tuples) {
            if (pre != null) {
                pre.next = cur;
                cur.prev = pre;
                cur.getGapLock().refresh();
            }
            pre = cur;
        }
        endDummy.prev = pre;
        endDummy.getGapLock().refresh();
        if (pre != null) {
            pre.next = endDummy;
        }
    }


    @Override
    public ITuple findTuple(ITuple searchKey) {
        ITuple<PrimaryTuple> search = primaryId2Tuples.ceiling((PrimaryTuple) searchKey);
        return search == null ? endDummy : search;
    }

    @Override
    public synchronized ITuple firstTuple() {
        return primaryId2Tuples.first();
    }

    /**
     * this method is used to build search tuple when do a index search
     * it only needs pid to find the first tuple it need
     *
     * @param pid
     * @return PrimaryTuple which only have pid (used to
     */
    public PrimaryTuple buildSearchTuple(int pid) {
        return new PrimaryTuple(pid);
    }


    @Override
    public ITuple insert(ITuple tuple) {
        int pid = (Integer) tuple.getOffsetValue(0);
        if (pid == SystemCatalog.NULL_PRIMARY_ID) {
            pid = primaryId2Tuples.isEmpty() ? 1 : primaryId2Tuples.last().id + 1;
        }
        PrimaryTuple add = new PrimaryTuple(pid, (String) tuple.getOffsetValue(1), (Integer) tuple.getOffsetValue(2),
                null, null, tuple.getTxnId());
        PrimaryTuple higher = (PrimaryTuple) findTuple(add);
        PrimaryTuple lower = higher.prev;
        higher.prev = add;
        add.next = higher;
        if (lower != null) {
            add.prev = lower;
            lower.next = add;
        }
        if (primaryId2Tuples.add(add)) {
            return add;
        }
        return null;
    }

    @Override
    public ITuple endDummy() {
        return endDummy;
    }

    @Override
    public IndexSearchResult firstTreeSearch(Predicate selectedPredicate) {
        ITuple<PrimaryTuple> nextTuple;
        boolean targetIsTreeSearchWithEqualExp = false;
        boolean lastTupleIsInvalid = false;
        if (selectedPredicate.expression == Expression.LESS || selectedPredicate.expression == Expression.LESS_EQUAL) {
            nextTuple = firstTuple();
        } else {
            PrimaryTuple searchTuple = buildSearchTuple((Integer) selectedPredicate.value);
            nextTuple = findTuple(searchTuple);
            boolean isEqual = nextTuple.compareTo(searchTuple) == 0;
            if (isEqual) {
                if (selectedPredicate.expression == Expression.LARGER) {
                    nextTuple = nextTuple.next();
                } else { // == and >=
                    if (selectedPredicate.expression == Expression.EQUAL) {
                        // no need to iterate one more
                        lastTupleIsInvalid = true;
                    }
                    targetIsTreeSearchWithEqualExp = true;
                }
            }
        }
        lastTupleIsInvalid |= (nextTuple == endDummy || !selectedPredicate.check(nextTuple));
        return new IndexSearchResult(targetIsTreeSearchWithEqualExp, lastTupleIsInvalid, nextTuple);
    }

    @Override
    public boolean containsKey(ITuple search) {
        return primaryId2Tuples.contains(search);
    }

    @Override
    public IPrimaryTuple buildInsertTuple(int pid, int txnId, Comparable... otherFields) {
        return new PrimaryTuple(pid, (String) otherFields[0], (Integer) otherFields[1], null, null, txnId);
    }
}
