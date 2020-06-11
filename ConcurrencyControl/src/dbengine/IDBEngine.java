package dbengine;

import dbengine.storage.ITuple;

public interface IDBEngine {
    String getNextRow();
    int updateNextRow();
    void commit();
    ITuple insert();
}
