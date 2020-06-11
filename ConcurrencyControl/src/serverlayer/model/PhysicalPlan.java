package serverlayer.model;

import dbengine.storage.ITuple;
import dbengine.storage.clusterIndex.PrimaryIndex;
import dbengine.storage.nonclusterIndex.NonUniqueIndex;
import dbengine.storage.tables.ITable;
import dbengine.transaction.IIsolationLevel;
import dbengine.transaction.LockMode;
import dbengine.transaction.TxnReadView;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class PhysicalPlan implements Iterator<ITuple> {

    public static final int allTableScan = 0;
    public static final int secondaryIndexScan = 1;
    public static final int primaryIndexScan = 2;
    
    public final int currentPlan;
    final List<Predicate> targetPredicates;
    LogicalPlan logicalPlan;

    boolean targetIsTreeSearchWithEqualExp = false;
    boolean firstIsInvalid = false;
    ITuple curTuple;
    private ITuple endDummy;

    public PhysicalPlan(List<Predicate> selectedPredicate, LogicalPlan logicalPlan) {

        targetPredicates = selectedPredicate;
        this.logicalPlan = logicalPlan;
        if (isSqlWithoutCRUD()) { // commit or begin
            currentPlan = -1;
            return;
        }
        if (selectedPredicate.isEmpty()) {
            currentPlan = allTableScan;
            curTuple = new DummyTuple(logicalPlan.table.getClusterIndex().firstTuple());
            endDummy = logicalPlan.table.getClusterIndex().endDummy();
        } else if (selectedPredicate.get(0).offset == 1) {
            currentPlan = secondaryIndexScan;
            NonUniqueIndex nameIndex = logicalPlan.table.getSecondaryIndex(selectedPredicate.get(0).offset);
            endDummy = nameIndex.endDummy();
            fulfill(nameIndex.firstSearch(selectedPredicate.get(0)));
        } else {
            currentPlan = primaryIndexScan;
            PrimaryIndex primaryIndex = logicalPlan.table.getClusterIndex();
            endDummy = primaryIndex.endDummy();
            fulfill(primaryIndex.firstSearch(selectedPredicate.get(0)));
        }
    }

    private boolean isSqlWithoutCRUD() {
        return logicalPlan.table == null;
    }

    private void fulfill(IndexSearchResult result) {
        curTuple = new DummyTuple(result.nextTuple);
        this.targetIsTreeSearchWithEqualExp = result.targetIsTreeSearchWithEqualExp;
        this.firstIsInvalid = result.lastTupleIsInvalid;
    }


    @Override
    public boolean hasNext() {
        return curTuple.next() != null;
    }

    @Override
    public ITuple next() {
        curTuple = curTuple.next();
        ITuple ret = curTuple;
        if (firstIsInvalid || curTuple == endDummy || !statisfyAllTargetPredicates(curTuple)) {
            curTuple = new DummyTuple(null);
        }
        return ret;
    }

    public boolean statisfyAllTargetPredicates(ITuple tuple) {
        boolean res = true;
        for (Predicate targetPredicate : targetPredicates) {
            res &= targetPredicate.check(tuple.getOffsetValue(targetPredicate.offset));
        }
        return res;
    }

    public LockMode getLockMode() {
        return logicalPlan.lockMode;
    }

    public int getTxnId() {
        return logicalPlan.txnId;
    }

    public ITuple findInPrimaryIndex(ITuple secondaryTuple) {
        PrimaryIndex primaryIndex = logicalPlan.table.getClusterIndex();
        return primaryIndex.findTuple(primaryIndex.buildSearchTuple((Integer) secondaryTuple.getOffsetValue(0)));
    }

    public List<Predicate> getProjection() {
        return logicalPlan.predicates;
    }

    public IIsolationLevel getIsolationLevel() {
        return logicalPlan.isolationLevel;
    }

    public List<UpdatedFunction> getUpdateFunction() { return logicalPlan.updatedValues; }

    public ITuple getInsertTuple() { return logicalPlan.insertTuple; }

    public ITable getTable() {return logicalPlan.table;}

    public TxnReadView getReadView() {
        return logicalPlan.readView;
    }

    public boolean isTargetIsTreeSearchWithEqualExp() {
        return targetIsTreeSearchWithEqualExp;
    }

    public boolean isEndDummy(ITuple ret) {
        return ret == endDummy;
    }

    public List<Integer> getSelectedColumns() { return logicalPlan.selectedColumns; }

}
