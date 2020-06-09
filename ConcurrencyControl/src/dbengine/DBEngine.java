package dbengine;

import dbengine.storage.IIndex;
import dbengine.storage.IUniqueIndex;
import dbengine.storage.tables.ITable;
import dbengine.transaction.LockMode;
import dbengine.transaction.LockStrategy;
import dbms.DBEngineGlobalEnvironment;
import dbms.SystemCatalog;
import serverlayer.model.PhysicalPlan;
import serverlayer.model.Predicate;
import serverlayer.model.UpdatedFunction;
import dbengine.storage.multipleversion.DeltaStorageRecord;
import dbengine.storage.ITuple;
import dbengine.storage.clusterIndex.IPrimaryTuple;
import dbengine.transaction.IIsolationLevel;

import java.util.ArrayList;
import java.util.List;

// ASSUMPTION : 因为MVCC情况下，更新索引列都会涉及删除后添加，为了简化MVCC的逻辑，我们这边的UPDATE只允许非索引列。
public class DBEngine implements IDBEngine{
    // 用来给加锁做优化， 第一个是树查找
    private boolean isFirstTuple, isFirstNotMeetCondition;
    PhysicalPlan physicalPlan;

    public DBEngine(PhysicalPlan physicalPlan) {
        if (physicalPlan != null) {
            this.physicalPlan = physicalPlan;
            isFirstTuple = true;
            isFirstNotMeetCondition = (physicalPlan.currentPlan != PhysicalPlan.allTableScan);
        }
    }

    public String getNextRow() {
        IPrimaryTuple ret = next();
        return ret == null ? null : ret.toString();
    }

    private IPrimaryTuple next() {
        if (!physicalPlan.hasNext()) return null;
        IIsolationLevel isolationLevel = physicalPlan.getIsolationLevel();

        ITuple ret = physicalPlan.next(), backup = null;

        Boolean needBackToPrimary = null; // 是否需要回表

        if (needBackToPrimary == null)  {
            needBackToPrimary = true;
            for (int requiredColumnOffset : physicalPlan.requiredColumnOffset()) {
                needBackToPrimary &= ret.haveOffsetValue(requiredColumnOffset);
            }
        }

        // enhance 负责 判断可见性， 如可见根据隔离级别来加锁
        LockStrategy strategy = new LockStrategy(isFirstTuple && physicalPlan.isTargetIsTreeSearchWithEqualExp());
        ret = isolationLevel.lockIfVisible(ret, physicalPlan.getLockMode(),
                physicalPlan.getReadView(),strategy);
        isFirstTuple = false;
        if (ret == null) { // 不可见
            return next();
        }
        // 可见，判断是否要回表
        boolean meetAllProjection = false;
        if (physicalPlan.isEndDummy(ret)) return null;

        if (!needBackToPrimary) { // go back to primary index
            if (physicalPlan.meetCondition(ret)) {
                backup = ret;
                ret = physicalPlan.findInPrimaryIndex(backup);
                strategy = new LockStrategy(true);
                ret = isolationLevel.lockIfVisible(ret, physicalPlan.getLockMode(), physicalPlan.getReadView(), strategy);
            }
        }
        // 负责判断谓词是否满足，不满足是否可以立刻释放锁
        meetAllProjection = true;
        List<Predicate> predicates = physicalPlan.getProjection();
        for (Predicate predicate : predicates) {
            meetAllProjection &= predicate.check(ret);
        }

        if (!meetAllProjection) {
            isolationLevel.unlockIfPossible(ret, backup, physicalPlan.getLockMode(), isFirstNotMeetCondition);
            isFirstNotMeetCondition = false;
            return next();
        }
        return (IPrimaryTuple) ret;
    }

    public void commit() {
        physicalPlan.getIsolationLevel().commit();
        DBEngineGlobalEnvironment.removeTxnId(physicalPlan.getTxnId());
    }

    public int updateNextRow() {
        IPrimaryTuple ret = next();
        return ret == null ? -1 : update(ret, physicalPlan.getUpdateFunction(), physicalPlan.getTxnId());
    }

    private int update(IPrimaryTuple tuple, List<UpdatedFunction> updatedFunctions, int txnId) {
        List<UpdatedFunction> toBeAppends = new ArrayList<>();
        boolean updateSuccess = false;
        int oldTxnId = tuple.getTxnId();
        for (UpdatedFunction uf : updatedFunctions) {
            toBeAppends.add(new UpdatedFunction(uf.getOffset(), tuple.getOffsetValue(uf.getOffset())));
            updateSuccess |= uf.update(tuple, txnId);
        }
        if (updateSuccess) {
            tuple.setPrevVersionRecord(new DeltaStorageRecord((DeltaStorageRecord) tuple.getPrevVersionRecord(), oldTxnId,
                    toBeAppends.toArray(new UpdatedFunction[0])));
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
                    isDuplicateKey |= ((IUniqueIndex)index).containsKey(toBeAdd);
                }
            }
            if (isDuplicateKey) return null;

            // add gap lock
            LockMode mode =  physicalPlan.getLockMode();
            assert mode == LockMode.INSERT_INTENTION;
            IIsolationLevel isolationLevel = physicalPlan.getIsolationLevel();
            isolationLevel.addLock(mode, gap);
            for (IIndex index : table.secondaryIndexes()) {
                physicalPlan.getIsolationLevel().lockIfVisible(index.findTuple(toBeAdd), mode, null,null);
            }
            ITuple ret;
            if ((ret = table.getClusterIndex().insert(toBeAdd)) == null) {
                throw new IllegalStateException("invalid area primary index");
            }
            isolationLevel.addLock(LockMode.EXCLUSIVE, ret);
            for (IIndex index : table.secondaryIndexes()) {
                if (index.insert(toBeAdd) == null) {
                    throw new IllegalStateException("invalid area secondary index");
                }
            }
            return toBeAdd;
        }
    }

    // public boolean insert()
}
