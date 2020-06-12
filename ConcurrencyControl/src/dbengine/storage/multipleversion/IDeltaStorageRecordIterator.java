package dbengine.storage.multipleversion;

/**
 * the class implements this function should have multiple version by txn Id
 * and it could get previous version and set previous version delta change record
 */
public interface IDeltaStorageRecordIterator extends IMultipleVersion {
    IDeltaStorageRecordIterator getPrevVersionRecord();

    void setPrevVersionRecord(IDeltaStorageRecordIterator deltaStorageRecord);

}
