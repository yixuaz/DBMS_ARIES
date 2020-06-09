package dbengine.transaction;

public class LockStrategy {
    boolean couldOptimizeToRecordLockOnly;

    public LockStrategy(boolean couldOptimizeToRecordLockOnly) {
        this.couldOptimizeToRecordLockOnly = couldOptimizeToRecordLockOnly;
    }
}
