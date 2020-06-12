package dbengine.storage.nonclusterindex;

import dbengine.storage.Expression;
import dbengine.storage.IIndex;
import dbengine.storage.ITuple;
import serverlayer.model.IndexSearchResult;
import serverlayer.model.Predicate;

import java.util.TreeSet;

import static dbms.SystemCatalog.END_DUMMY_TXN_ID_TAG;

public class NonUniqueIndex implements IIndex {
    private static final NonUniqueIndexTuple endDummy
            = new NonUniqueIndexTuple(null, 0, END_DUMMY_TXN_ID_TAG);
    private TreeSet<NonUniqueIndexTuple> datas = new TreeSet<>();

    public NonUniqueIndex() {
        datas.add(new NonUniqueIndexTuple("aaa", 1, 0));
        datas.add(new NonUniqueIndexTuple("bbb", 2, 0));
        datas.add(new NonUniqueIndexTuple("bbb", 3, 0));
        datas.add(new NonUniqueIndexTuple("ccc", 7, 0));
        NonUniqueIndexTuple pre = null;
        for (NonUniqueIndexTuple cur : datas) {
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
        ITuple<NonUniqueIndexTuple> ret;
        if (searchKey instanceof NonUniqueIndexTuple) {
            ret = datas.ceiling((NonUniqueIndexTuple) searchKey);
        } else {
            ret = datas.ceiling(buildSearchTuple((String) searchKey.getOffsetValue(1), (Integer) searchKey.getOffsetValue(0)));
        }
        return ret == null ? endDummy : ret;
    }

    @Override
    public ITuple firstTuple() {
        return datas.first();
    }


    @Override
    public ITuple insert(ITuple tuple) {
        NonUniqueIndexTuple insert = new NonUniqueIndexTuple(tuple);
        NonUniqueIndexTuple higher = (NonUniqueIndexTuple) findTuple(insert);
        NonUniqueIndexTuple lower = higher.prev;
        insert.next = higher;
        higher.prev = insert;
        if (lower != null) {
            lower.next = insert;
            insert.prev = lower;
        }
        if (datas.add(insert)) {
            return insert;
        }
        return null;
    }

    @Override
    public ITuple endDummy() {
        return endDummy;
    }

    @Override
    public IndexSearchResult firstTreeSearch(Predicate selectedPredicate) {
        ITuple<NonUniqueIndexTuple> nextTuple = null;
        if (selectedPredicate.expression == Expression.LESS || selectedPredicate.expression == Expression.LESS_EQUAL) {
            nextTuple = firstTuple();
        } else if (selectedPredicate.expression == Expression.LARGER) {
            NonUniqueIndexTuple searchTuple = buildSearchTuple((String) selectedPredicate.value, Integer.MAX_VALUE);
            nextTuple = findTuple(searchTuple);
            if (nextTuple != endDummy && nextTuple.compareTo(searchTuple) == 0) {
                nextTuple = nextTuple.next();
            }
        } else {
            nextTuple = findTuple(buildSearchTuple((String) selectedPredicate.value, Integer.MIN_VALUE));
        }
        // non unique index always need previous gap lock (targetIsTreeSearchWithEqualExp should be false)
        return new IndexSearchResult(false,
                nextTuple == endDummy || !selectedPredicate.check(nextTuple), nextTuple);
    }

    private NonUniqueIndexTuple buildSearchTuple(String name, int id) {
        return new NonUniqueIndexTuple(name, id, -1);
    }

}
