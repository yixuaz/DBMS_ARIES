package dbms;

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class DBEngineGlobalEnvironment {
    static final ConcurrentSkipListSet<Integer> unCommitedTxnGroup = new ConcurrentSkipListSet<>();

    public static SortedSet<Integer> getUnCommitedTxnGroup() {
        return Collections.unmodifiableSortedSet(unCommitedTxnGroup.clone());
    }

    public static void addTxnId(int txnId) {
        unCommitedTxnGroup.add(txnId);
    }

    public static void removeTxnId(int txnId) {
        unCommitedTxnGroup.remove(txnId);
    }
}
