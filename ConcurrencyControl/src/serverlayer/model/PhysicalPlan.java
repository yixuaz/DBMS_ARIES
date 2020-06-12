package serverlayer.model;

import dbengine.storage.IIndex;
import dbengine.storage.IPrimaryIndex;
import dbengine.storage.ITuple;
import dbengine.storage.tables.ITable;
import dbengine.transaction.IIsolationLevel;
import dbengine.transaction.model.LockMode;
import dbengine.transaction.model.TxnReadView;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class PhysicalPlan implements Iterator<ITuple> {

    public static final int ALL_TABLE_SCAN = 0;
    public static final int SECONDARY_INDEX_SCAN = 1;
    public static final int PRIMARY_INDEX_SCAN = 2;
    
    public final int currentPlan;

    private final LogicalPlan logicalPlan;
    private final List<Predicate> targetPredicates;
    private final ITuple endDummy;

    private boolean targetIsTreeSearchWithEqualExp;
    private boolean firstIsInvalid;
    private ITuple curTuple;


    public PhysicalPlan(List<Predicate> selectedPredicate, LogicalPlan logicalPlan) {

        targetPredicates = selectedPredicate;
        this.logicalPlan = logicalPlan;
        if (isSqlWithoutCRUD()) { // commit or begin
            currentPlan = -1;
            endDummy = null;
            return;
        }
        if (selectedPredicate.isEmpty()) {
            currentPlan = ALL_TABLE_SCAN;
            curTuple = new DummyTuple(logicalPlan.table.getClusterIndex().firstTuple());
            endDummy = logicalPlan.table.getClusterIndex().endDummy();
        } else if (selectedPredicate.get(0).offset == 1) {
            currentPlan = SECONDARY_INDEX_SCAN;
            IIndex nameIndex = logicalPlan.table.getSecondaryIndex(selectedPredicate.get(0).offset);
            endDummy = nameIndex.endDummy();
            fulfill(nameIndex.firstTreeSearch(selectedPredicate.get(0)));
        } else {
            currentPlan = PRIMARY_INDEX_SCAN;
            IPrimaryIndex primaryIndex = logicalPlan.table.getClusterIndex();
            endDummy = primaryIndex.endDummy();
            fulfill(primaryIndex.firstTreeSearch(selectedPredicate.get(0)));
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
        if (curTuple == null) {
            throw new NoSuchElementException();
        }
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
        IPrimaryIndex primaryIndex = logicalPlan.table.getClusterIndex();
        return primaryIndex.findTuple(primaryIndex.buildSearchTuple((Integer) secondaryTuple.getOffsetValue(0)));
    }

    public List<Predicate> getPredicates() {
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
