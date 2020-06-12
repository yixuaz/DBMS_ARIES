package dbengine;

import dbengine.storage.IIndex;
import dbengine.storage.ITuple;
import dbengine.storage.IUniqueIndex;
import dbengine.storage.clusterindex.IPrimaryTuple;
import dbengine.storage.multipleversion.DeltaStorageRecord;
import dbengine.storage.tables.ITable;
import dbengine.transaction.IIsolationLevel;
import dbengine.transaction.model.LockMode;
import dbengine.transaction.model.LockStrategy;
import dbms.DBEngineGlobalEnvironment;
import serverlayer.model.PhysicalPlan;
import serverlayer.model.Predicate;
import serverlayer.model.UpdatedFunction;

import java.util.ArrayList;
import java.util.List;

// ASSUMPTION : 因为MVCC情况下，更新索引列都会涉及删除后添加，为了简化MVCC的逻辑，我们这边的UPDATE只允许非索引列。
public class DBEngine implements IDBEngine {
    // 用来给加锁做优化， 第一个是树查找
    private boolean isFirstTuple;
    private boolean isFirstNotMeetCondition;
    PhysicalPlan physicalPlan;

    public DBEngine(PhysicalPlan physicalPlan) {
        if (physicalPlan != null) {
            this.physicalPlan = physicalPlan;
            isFirstTuple = true;
            isFirstNotMeetCondition = (physicalPlan.currentPlan != PhysicalPlan.ALL_TABLE_SCAN);
        }
    }

    public String getNextRow() {
        ITuple ret = next();
        return ret == null ? null : ret.toString(physicalPlan.getSelectedColumns());
    }

    private ITuple next() {
        if (!physicalPlan.hasNext())
            return null;
        IIsolationLevel isolationLevel = physicalPlan.getIsolationLevel();

        ITuple ret = physicalPlan.next();
        ITuple backup = null;

        // 1. check if the index could cover all the require columns
        Boolean isIndexCoverRequiredColumns = null;

        if (isIndexCoverRequiredColumns == null) {
            isIndexCoverRequiredColumns = false;
            for (int requiredColumnOffset : physicalPlan.getSelectedColumns()) {
                isIndexCoverRequiredColumns |= !ret.offsetExists(requiredColumnOffset);
            }
            for (Predicate predicateColumnOffset : physicalPlan.getPredicates()) {
                isIndexCoverRequiredColumns |= !ret.offsetExists(predicateColumnOffset.getOffset());
            }
            isIndexCoverRequiredColumns |= isUpdateSQL(physicalPlan.getUpdateFunction());
        }

        // 2. check need to add lock and check if have multiple version, it should return visible one
        LockStrategy strategy = new LockStrategy(isFirstTuple && physicalPlan.isTargetIsTreeSearchWithEqualExp());
        ret = isolationLevel.lockIfVisible(ret, physicalPlan.getLockMode(),
                physicalPlan.getReadView(), strategy);
        isFirstTuple = false;
        if (ret == null) { // invisible
            return next();
        }
        // 3. if visible then check it need to back to primary
        boolean meetAllProjection = false;
        if (physicalPlan.isEndDummy(ret)) return null;

        if (isIndexCoverRequiredColumns && physicalPlan.statisfyAllTargetPredicates(ret)) { // go back to primary index
            backup = ret;
            ret = physicalPlan.findInPrimaryIndex(backup);
            strategy = new LockStrategy(true);
            ret = isolationLevel.lockIfVisible(ret, physicalPlan.getLockMode(), physicalPlan.getReadView(), strategy);
        }
        // 4. check if all predicate is matched, if not release some lock to improve concurrency without break correctness
        meetAllProjection = true;
        List<Predicate> predicates = physicalPlan.getPredicates();
        for (Predicate predicate : predicates) {
            meetAllProjection &= predicate.check(ret);
        }
        if (!meetAllProjection) {
            isolationLevel.unlockIfPossible(ret, backup, physicalPlan.getLockMode(), isFirstNotMeetCondition);
            isFirstNotMeetCondition = false;
            return next();
        }
        return ret;
    }

    private boolean isUpdateSQL(List<UpdatedFunction> updateFunction) {
        return updateFunction != null && !updateFunction.isEmpty();
    }

    public void commit() {
        physicalPlan.getIsolationLevel().commit();
        DBEngineGlobalEnvironment.removeTxnId(physicalPlan.getTxnId());
    }

    public int updateNextRow() {
        IPrimaryTuple ret = (IPrimaryTuple) next();
        return ret == null ? -1 : update(ret, physicalPlan.getUpdateFunction(), physicalPlan.getTxnId());
    }

    private int update(IPrimaryTuple tuple, List<UpdatedFunction> updatedFunctions, int txnId) {
        List<UpdatedFunction> toBeAppendDeltaChanges = new ArrayList<>();
        boolean updateSuccess = false;
        int oldTxnId = tuple.getTxnId();
        for (UpdatedFunction uf : updatedFunctions) {
            toBeAppendDeltaChanges.add(new UpdatedFunction(uf.getOffset(), tuple.getOffsetValue(uf.getOffset())));
            updateSuccess |= uf.update(tuple, txnId);
        }
        if (updateSuccess) {
            tuple.setPrevVersionRecord(new DeltaStorageRecord((DeltaStorageRecord) tuple.getPrevVersionRecord(), oldTxnId,
                    toBeAppendDeltaChanges.toArray(new UpdatedFunction[0])));
        }
        return updateSuccess ? 1 : 0;
    }

    // assumption, no need auto generated primary key in insert
    public ITuple insert() {
        ITable table = physicalPlan.getTable();
        ITuple toBeAdd = physicalPlan.getInsertTuple();
        ITuple gap = table.getClusterIndex().findTuple(toBeAdd);
        synchronized (gap) {
            // check duplicate
            boolean isDuplicateKey = table.getClusterIndex().containsKey(toBeAdd);
            for (IIndex index : table.secondaryIndexes()) {
                if (index instanceof IUniqueIndex) {
                    isDuplicateKey |= ((IUniqueIndex) index).containsKey(toBeAdd);
                }
            }
            if (isDuplicateKey) return null;

            // add gap lock
            LockMode mode = physicalPlan.getLockMode();
            assert mode == LockMode.INSERT_INTENTION;
            IIsolationLevel isolationLevel = physicalPlan.getIsolationLevel();
            isolationLevel.addLock(mode, gap);
            for (IIndex index : table.secondaryIndexes()) {
                physicalPlan.getIsolationLevel().lockIfVisible(index.findTuple(toBeAdd), mode, null, null);
            }
            // insert then add record lock
            ITuple ret = table.getClusterIndex().insert(toBeAdd);
            assert ret != null;
            isolationLevel.addLock(LockMode.EXCLUSIVE, ret);
            for (IIndex index : table.secondaryIndexes()) {
                if (index.insert(ret) == null) {
                    throw new IllegalStateException("invalid area secondary index");
                }
            }
            return ret;
        }
    }
}
