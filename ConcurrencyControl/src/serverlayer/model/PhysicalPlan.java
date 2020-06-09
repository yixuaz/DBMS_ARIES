package serverlayer.model;

import dbengine.storage.Expression;
import dbengine.storage.ITuple;
import dbengine.storage.clusterIndex.PrimaryIndex;
import dbengine.storage.nonclusterIndex.NonUniqueIndex;
import dbengine.storage.tables.ITable;
import dbengine.transaction.IIsolationLevel;
import dbengine.transaction.LockMode;
import dbengine.transaction.TxnReadView;

import java.util.Iterator;
import java.util.List;

public class PhysicalPlan implements Iterator<ITuple> {

    public static final int allTableScan = 0;
    public static final int secondaryIndexScan = 1;
    public static final int primaryIndexScan = 2;
    
    public final int currentPlan;
    Predicate targetPredicate;
    LogicalPlan logicalPlan;

    boolean targetIsTreeSearchWithEqualExp = false;
    boolean lastIsInvalid = false;
    ITuple curTuple;
    ITuple nextTuple;
    private ITuple endDummy;

    public PhysicalPlan(Predicate selectedPredicate, LogicalPlan logicalPlan) {
        targetPredicate = selectedPredicate;
        this.logicalPlan = logicalPlan;
        if (isSqlWithoutCRUD()) { // commit or begin
            currentPlan = -1;
            return;
        }
        if (selectedPredicate == null) {
            currentPlan = allTableScan;
           // nextTuple = logicalPlan.table.getClusterIndex().firstTuple();
            curTuple = new DummyTuple(logicalPlan.table.getClusterIndex().firstTuple());
            endDummy = logicalPlan.table.getClusterIndex().endDummy();
        } else if (selectedPredicate.offset == 1) {
            currentPlan = secondaryIndexScan;
            NonUniqueIndex nameIndex = logicalPlan.table.getSecondaryIndex(selectedPredicate.offset);
            endDummy = nameIndex.endDummy();
            fulfill(nameIndex.firstSearch(selectedPredicate));
        } else {
            currentPlan = primaryIndexScan;
            PrimaryIndex primaryIndex = logicalPlan.table.getClusterIndex();
            endDummy = primaryIndex.endDummy();
            fulfill(primaryIndex.firstSearch(selectedPredicate));
        }
    }

    private boolean isSqlWithoutCRUD() {
        return logicalPlan.table == null;
    }

    private void fulfill(IndexSearchResult result) {
        curTuple = new DummyTuple(result.nextTuple);
        // nextTuple = result.nextTuple;
        this.targetIsTreeSearchWithEqualExp = result.targetIsTreeSearchWithEqualExp;
        this.lastIsInvalid = result.lastTupleIsInvalid;
    }


    @Override
    public boolean hasNext() {
        // return nextTuple != null;
        return curTuple.next() != null;
    }

    @Override
    public ITuple next() {
//        ITuple ret = nextTuple;
//        boolean preLastIsInvalid = lastIsInvalid;
//        nextTuple =  nextTuple.next();
//
//        if (targetPredicate != null) {
//            lastIsInvalid = (nextTuple == endDummy) || (nextTuple == null) ||
//                    !(targetPredicate.check(nextTuple.getOffsetValue(targetPredicate.offset)));
//        }
//        // 索引上的范围查询会访问到不满足条件的第一个值为止
//        if (preLastIsInvalid) {
//            nextTuple = null;
//        }
//
//        return ret;
        curTuple = curTuple.next();
        ITuple ret = curTuple;
        if (lastIsInvalid || curTuple == endDummy ||
                (targetPredicate != null
                        && !(targetPredicate.check(curTuple.getOffsetValue(targetPredicate.offset))))) {
            curTuple = new DummyTuple(null);
        }
        return ret;
    }

    public int[] requiredColumnOffset() {
        return new int[]{0,1,2};
    } // should be get from logical plan

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

    public ITuple getEndDummy() {
        return endDummy;
    }

    public boolean meetCondition(ITuple find) {
        if (targetPredicate == null) return true;
        return targetPredicate.check(find);
    }

    public boolean isEqualExpPlan() {
        return targetPredicate != null && Expression.isEqual(targetPredicate.expression);
    }
}
