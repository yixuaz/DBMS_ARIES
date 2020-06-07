package dbengine.storage.clusterIndex;

import dbengine.storage.IIndex;
import dbengine.storage.ITuple;

import java.util.TreeSet;

import static dbms.SystemCatalog.END_DUMMY_TXN_ID_TAG;

public class PrimaryIndex implements IIndex {
    private static final PrimaryTuple endDummy = new PrimaryTuple(0, null, 0,
            null, null, END_DUMMY_TXN_ID_TAG);
    TreeSet<PrimaryTuple> primaryId2Tuple = new TreeSet<>();
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
            }
            pre = cur;
        }
        endDummy.prev = pre;
        pre.next = endDummy;
    }

    @Override
    public ITuple findTuple(ITuple searchKey) {
        ITuple search = primaryId2Tuple.ceiling((PrimaryTuple) searchKey);
        return search == null ? endDummy : search;
    }

    @Override
    public ITuple firstTuple() {
        return primaryId2Tuple.first();
    }


    public PrimaryTuple buildSearchTuple(int pid) {
        return new PrimaryTuple(pid, "",0,null, null, -1);
    }

    @Override
    public ITuple insert(ITuple tuple) {
//        String pid = (String) tuple.getOffsetValue(0);
//        int primaryId;
//        if (pid == null) {
//            primaryId = primaryId2Tuple.isEmpty() ? 1 : primaryId2Tuple.lastKey() + 1;
//        } else {
//            primaryId = Integer.parseInt(pid);
//            if (primaryId2Tuple.containsKey(primaryId)) {
//                throw new IllegalStateException("unique index conflict");
//            }
//        }
//        // TODO: set gap lock
//        PrimaryTuple add = new PrimaryTuple(primaryId, (String) tuple.getOffsetValue(1), (Integer)tuple.getOffsetValue(2),
//                primaryId2Tuple.lower(primaryId).getValue(),
//                primaryId2Tuple.higherEntry(primaryId).getValue(), tuple.getTxnId());
//        primaryId2Tuple.put(primaryId, add);
//        return add;
        return null;
    }

    @Override
    public ITuple endDummy() {
        return endDummy;
    }
}
