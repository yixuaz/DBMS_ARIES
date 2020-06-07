package serverlayer.model;

import dbengine.storage.tables.ITable;
import dbengine.transaction.IIsolationLevel;
import dbengine.transaction.LockMode;
import dbengine.transaction.TxnReadView;

import java.util.List;

public class LogicalPlan {
    ITable table;
    List<Predicate> predicates;
    List<Integer> selectedColumns;
    List<UpdatedFunction> updatedValues;
    LockMode lockMode;
    int txnId;
    IIsolationLevel isolationLevel;
    TxnReadView readView;

    public void setTable(ITable table) {
        this.table = table;
    }

    public void setPredicates(List<Predicate> predicates) {
        this.predicates = predicates;
    }

    public void setSelectedColumns(List<Integer> selectedColumns) {
        this.selectedColumns = selectedColumns;
    }

    public void setUpdatedValues(List<UpdatedFunction> updatedValues) {
        this.updatedValues = updatedValues;
    }

    public void setLockMode(LockMode lockMode) {
        this.lockMode = lockMode;
    }

    public void setTxnId(int txnId) {
        this.txnId = txnId;
    }

    public void setIsolationLevel(IIsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public ITable getTable() {
        return table;
    }

    public List<Predicate> getPredicates() {
        return predicates;
    }

    public List<Integer> getSelectedColumns() {
        return selectedColumns;
    }

    public List<UpdatedFunction> getUpdatedValues() {
        return updatedValues;
    }

    public LockMode getLockMode() {
        return lockMode;
    }

    public int getTxnId() {
        return txnId;
    }

    public IIsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public void setReadView(TxnReadView readView) {
        this.readView = readView;
    }
}
