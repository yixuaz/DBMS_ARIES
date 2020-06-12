package dbengine.transaction.model;

public class LockStrategy {
    public final boolean couldOptimizeToRecordLockOnly;
    public LockStrategy(boolean couldOptimizeToRecordLockOnly) {
        this.couldOptimizeToRecordLockOnly = couldOptimizeToRecordLockOnly;
    }
}
