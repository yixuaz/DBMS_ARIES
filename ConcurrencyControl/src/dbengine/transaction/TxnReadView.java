package dbengine.transaction;

import dbms.DBEngineGlobalEnvironment;
import dbms.SystemCatalog;

import java.util.SortedSet;

public class TxnReadView {
    final SortedSet<Integer> unCommittedTxnGroup;
    final int lowWaterMark, highWaterMark, createdTxnId;

    public TxnReadView(int curTxnId) {
        unCommittedTxnGroup = DBEngineGlobalEnvironment.getUnCommitedTxnGroup();
        lowWaterMark = unCommittedTxnGroup.first();
        highWaterMark = SystemCatalog.getMaxTxnId();
        createdTxnId = curTxnId;
    }

    public boolean isVisble(int txnId) {
        if (txnId == createdTxnId) return true;
        if (txnId < lowWaterMark) return true;
        if (txnId > highWaterMark) return false;
        return !unCommittedTxnGroup.contains(txnId);
    }
}
