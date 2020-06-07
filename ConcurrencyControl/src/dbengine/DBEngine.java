package dbengine;

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
            isFirstNotMeetCondition = physicalPlan.currentPlan != PhysicalPlan.allTableScan;
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
        LockStrategy strategy = new LockStrategy(isFirstTuple && physicalPlan.isTargetIsTreeSearch(),
                physicalPlan.meetCondition(ret));
        ret = isolationLevel.lockIfVisible(ret, physicalPlan.getLockMode(),
                physicalPlan.getReadView(),strategy);
        if (ret == SystemCatalog.INVALID_TUPLE) return null;
        isFirstTuple = false;
        if (ret == null) { // 不可见
            return next();
        }
        // 可见，判断是否要回表
        if (!needBackToPrimary) { // go back to primary index
            if (physicalPlan.isEndDummy(ret)) return null;
            backup = ret;
            ret = physicalPlan.findInPrimaryIndex(backup);
            strategy = new LockStrategy(true, true);
            LockMode mode = physicalPlan.getLockMode();
            if (mode != null) mode = LockMode.SHARE;
            ret = isolationLevel.lockIfVisible(ret, mode, physicalPlan.getReadView(), strategy);
        }
        // 负责判断谓词是否满足，不满足是否可以立刻释放锁
        boolean meetAllProjection = !physicalPlan.isEndDummy(ret);
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

    public void commit(IIsolationLevel isolationLevel, int txnId) {
        isolationLevel.commit();
        DBEngineGlobalEnvironment.removeTxnId(txnId);
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

    // public boolean insert()
}
