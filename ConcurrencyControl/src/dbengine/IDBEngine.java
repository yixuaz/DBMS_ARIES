package dbengine;

import serverlayer.model.PhysicalPlan;
import dbengine.transaction.IIsolationLevel;

public interface IDBEngine {
    String getNextRow();
    int updateNextRow();
    void commit(IIsolationLevel isolationLevel, int txnId);
}
