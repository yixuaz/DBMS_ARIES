package dbengine.storage.multipleversion;

import dbengine.storage.clusterIndex.IPrimaryTuple;
import serverlayer.model.UpdatedFunction;

import java.util.Arrays;
import java.util.List;

public class DeltaStorageRecord implements IDeltaStorageRecordUpdater, IDeltaStorageRecordIterator {
    DeltaStorageRecord next;
    List<UpdatedFunction> deltaChange;
    int txnId;

    public DeltaStorageRecord(DeltaStorageRecord next, int txnId, UpdatedFunction... deltaChanges) {
        this.next = next;
        this.deltaChange = Arrays.asList(deltaChanges);
        this.txnId = txnId;
    }

    @Override
    public void setPrevVersionRecord(IDeltaStorageRecordIterator deltaStorageRecord) {
        next = (DeltaStorageRecord) deltaStorageRecord;
    }

    @Override
    public DeltaStorageRecord getPrevVersionRecord() {
        return next;
    }

    @Override
    public boolean update(IPrimaryTuple clone) {
        boolean success = false;
        for (UpdatedFunction uf : deltaChange) {
            success |= uf.update(clone, clone.getTxnId());
        }
        return success;
    }

    @Override
    public int getTxnId() {
        return txnId;
    }
}
