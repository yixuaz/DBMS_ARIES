package dbengine.storage.multipleversion;

public interface IDeltaStorageRecordIterator extends IMultipleVersion {
    IDeltaStorageRecordIterator getPrevVersionRecord();
    void setPrevVersionRecord(IDeltaStorageRecordIterator deltaStorageRecord);

}
