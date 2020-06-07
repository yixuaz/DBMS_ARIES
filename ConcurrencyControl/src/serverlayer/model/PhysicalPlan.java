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
    boolean targetIsTreeSearch = false;
    boolean lastIsInvalid = false;
    private ITuple endDummy;

    public PhysicalPlan(Predicate selectedPredicate, LogicalPlan logicalPlan) {
        targetPredicate = selectedPredicate;
        this.logicalPlan = logicalPlan;
        if (selectedPredicate == null) {
            currentPlan = allTableScan;
            nextTuple = logicalPlan.table.getClusterIndex().firstTuple();
            endDummy = logicalPlan.table.getClusterIndex().endDummy();
        } else if (selectedPredicate.offset == 1) {
            currentPlan = secondaryIndexScan;
            NonUniqueIndex nameIndex = logicalPlan.table.getSecondaryIndex(selectedPredicate.offset);
            endDummy = nameIndex.endDummy();
            if (selectedPredicate.expression == Expression.LESS || selectedPredicate.expression == Expression.LESS_EQUAL) {
                nextTuple = nameIndex.firstTuple();
            } else if (selectedPredicate.expression == Expression.LARGER) {
                ITuple searchTuple = nameIndex.buildSearchTuple((String) selectedPredicate.value, Integer.MAX_VALUE);
                nextTuple = nameIndex.findTuple(searchTuple);
                if (nextTuple != endDummy && nextTuple.compareTo(searchTuple) == 0) {
                    nextTuple = nextTuple.next();
                } else {
                    targetIsTreeSearch = nextTuple != endDummy;
                }
            } else {
                nextTuple = nameIndex.findTuple(nameIndex.buildSearchTuple((String) selectedPredicate.value, Integer.MIN_VALUE));
                targetIsTreeSearch = nextTuple != endDummy;
            }
        } else {
            currentPlan = primaryIndexScan;
            PrimaryIndex primaryIndex = logicalPlan.table.getClusterIndex();
            endDummy = primaryIndex.endDummy();
            if (selectedPredicate.expression == Expression.LESS || selectedPredicate.expression == Expression.LESS_EQUAL) {
                nextTuple = primaryIndex.firstTuple();
            } else  {
                ITuple searchTuple = primaryIndex.buildSearchTuple((Integer) selectedPredicate.value);
                nextTuple = primaryIndex.findTuple(searchTuple);
                if (selectedPredicate.expression == Expression.LARGER && nextTuple.compareTo(searchTuple) == 0) {
                    nextTuple = nextTuple.next();
                } else {
                    targetIsTreeSearch = nextTuple != endDummy;
                }
            }
        }
    }

    ITuple nextTuple;

    @Override
    public boolean hasNext() {
        return nextTuple != null;
    }

    @Override
    public ITuple next() {
        ITuple ret = nextTuple;

        nextTuple =  nextTuple.next();
        if (lastIsInvalid && targetPredicate != null && nextTuple != null && nextTuple != endDummy &&
                (lastIsInvalid = !(targetPredicate.check(nextTuple.getOffsetValue(targetPredicate.offset))))) {
            nextTuple = null;
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

    public TxnReadView getReadView() {
        return logicalPlan.readView;
    }

    public boolean isTargetIsTreeSearch() {
        return targetIsTreeSearch;
    }

    public boolean isEndDummy(ITuple ret) {
        return ret == endDummy;
    }

    public ITuple getEndDummy() {
        return endDummy;
    }

    public boolean meetCondition(ITuple find) {
        if (targetPredicate == null || find == endDummy) return false;
        return targetPredicate.check(find);
    }
}
