package dbengine.storage.nonclusterIndex;

import dbengine.storage.IIndex;
import dbengine.storage.ITuple;
import dbengine.storage.GapLock;
import dbengine.storage.multipleversion.IMultipleVersion;

import java.util.TreeSet;

import static dbms.SystemCatalog.END_DUMMY_TXN_ID_TAG;

public class NonUniqueIndex implements IIndex {
    private static final NonUniqueIndexTuple endDummy
            = new NonUniqueIndexTuple(null,0, END_DUMMY_TXN_ID_TAG);
    TreeSet<NonUniqueIndexTuple> datas = new TreeSet<>();
    {
        datas.add(new NonUniqueIndexTuple("aaa", 1,0));
        datas.add(new NonUniqueIndexTuple("bbb", 2,0));
        datas.add(new NonUniqueIndexTuple("bbb", 3,0));
        datas.add(new NonUniqueIndexTuple("ccc", 7,0));
        NonUniqueIndexTuple pre = null;
        for (NonUniqueIndexTuple cur : datas) {
            if (pre != null) {
                pre.next = cur;
                cur.prev = pre;
            }
            pre = cur;
        }
        endDummy.prev = pre;
        pre.next = endDummy;
    }



    @Override
    public ITuple findTuple(ITuple searchKey) {
        ITuple ret = datas.ceiling((NonUniqueIndexTuple) searchKey);
        return ret == null ? endDummy : ret;
    }

    @Override
    public ITuple firstTuple() {
        return datas.first();
    }


    @Override
    public ITuple insert(ITuple tuple) {
        NonUniqueIndexTuple insert = new NonUniqueIndexTuple(tuple);
        datas.add(insert);
        return insert;
    }

    @Override
    public ITuple endDummy() {
        return endDummy;
    }

    public NonUniqueIndexTuple buildSearchTuple(String name, int id) {
        return new NonUniqueIndexTuple(name, id, -1);
    }

}
