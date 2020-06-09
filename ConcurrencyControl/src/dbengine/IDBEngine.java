package dbengine;

import dbengine.storage.ITuple;
import serverlayer.model.PhysicalPlan;
import dbengine.transaction.IIsolationLevel;

public interface IDBEngine {
    String getNextRow();
    int updateNextRow();
    void commit();
    ITuple insert();
}
