package dbengine.storage.clusterIndex;

import dbengine.storage.Expression;
import dbengine.storage.IIndex;
import dbengine.storage.IPrimaryIndex;
import dbengine.storage.ITuple;
import dbengine.storage.IUniqueIndex;
import dbengine.transaction.LockMode;
import dbms.SystemCatalog;
import serverlayer.model.IndexSearchResult;
import serverlayer.model.Predicate;

import java.util.TreeSet;

import static dbms.SystemCatalog.END_DUMMY_TXN_ID_TAG;

public class PrimaryIndex implements IPrimaryIndex {
    private static final PrimaryTuple endDummy = new PrimaryTuple(0, null, 0,
            null, null, END_DUMMY_TXN_ID_TAG);
    final TreeSet<PrimaryTuple> primaryId2Tuple = new TreeSet<>();
    {
        primaryId2Tuple.add(new PrimaryTuple(1, "aaa", 100, null, null, 0));
        primaryId2Tuple.add(new PrimaryTuple(2, "bbb", 200, null, null, 0));
        primaryId2Tuple.add(new PrimaryTuple(3, "bbb", 300, null, null, 0));
        primaryId2Tuple.add(new PrimaryTuple(7, "ccc", 200, null, null, 0));
        PrimaryTuple pre = null;
        for (PrimaryTuple cur : primaryId2Tuple) {
            if (pre != null) {
                pre.next = cur;
                cur.prev = pre;
                cur.getGapLock().refresh();
            }
            pre = cur;
        }
        endDummy.prev = pre;
        endDummy.getGapLock().refresh();
        pre.next = endDummy;
    }

    @Override
    public synchronized ITuple findTuple(ITuple searchKey) {
        ITuple search = primaryId2Tuple.ceiling((PrimaryTuple) searchKey);
        return search == null ? endDummy : search;
    }

    @Override
    public synchronized ITuple firstTuple() {
        return primaryId2Tuple.first();
    }


    public PrimaryTuple buildSearchTuple(int pid) {
        return new PrimaryTuple(pid, "",0,null, null, -1);
    }


    @Override
    public ITuple insert(ITuple tuple) {
        int pid = (Integer) tuple.getOffsetValue(0);
        if (pid == SystemCatalog.NULL_PRIMARY_ID) {
            tuple.setOffsetValue(0, primaryId2Tuple.isEmpty() ? 1 : primaryId2Tuple.last().id + 1, tuple.getTxnId());
        }
        PrimaryTuple add = new PrimaryTuple(pid,  (String) tuple.getOffsetValue(1), (Integer) tuple.getOffsetValue(2),
                null, null, tuple.getTxnId());
        PrimaryTuple higher = (PrimaryTuple) findTuple(add);
        PrimaryTuple lower = higher.prev;
        higher.prev = add;
        add.next = higher;
        if (lower != null) {
            add.prev = lower;
            lower.next = add;
        }
        if (primaryId2Tuple.add(add)) {
            return add;
        }
        return null;
    }

    @Override
    public ITuple endDummy() {
        return endDummy;
    }

    @Override
    public IndexSearchResult firstSearch(Predicate selectedPredicate) {
        ITuple nextTuple;
        boolean targetIsTreeSearchWithEqualExp = false, lastTupleIsInvalid = false;
        if (selectedPredicate.expression == Expression.LESS || selectedPredicate.expression == Expression.LESS_EQUAL) {
            nextTuple = firstTuple();
        } else  {
            ITuple searchTuple = buildSearchTuple((Integer) selectedPredicate.value);
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
        return new IndexSearchResult(targetIsTreeSearchWithEqualExp,
                lastTupleIsInvalid, nextTuple);
    }

    @Override
    public boolean containsKey(ITuple search) {
        return primaryId2Tuple.contains(search);
    }

    @Override
    public IPrimaryTuple buildInsertTuple(int pid, int txnId, Comparable... otherFields) {
        return new PrimaryTuple(pid, (String) otherFields[0], (Integer) otherFields[1],null, null, txnId);
    }
}
