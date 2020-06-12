package dbengine.storage.multipleversion;

/**
 * use txn id to record version info
 */
public interface IMultipleVersion {
    int getTxnId();
}
