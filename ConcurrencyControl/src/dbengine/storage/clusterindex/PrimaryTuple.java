package dbengine.storage.clusterindex;

import dbengine.storage.GapLock;
import dbengine.storage.ITuple;
import dbengine.storage.multipleversion.IDeltaStorageRecordIterator;
import dbengine.storage.multipleversion.IDeltaStorageRecordUpdater;
import util.MyReadWriteLock;

import java.util.concurrent.locks.ReadWriteLock;

public class PrimaryTuple implements IPrimaryTuple<PrimaryTuple> {
    PrimaryTuple next;
    PrimaryTuple prev;
    int id;
    private String name;
    private int num;
    private IDeltaStorageRecordIterator prevVersionRecord;
    private int txnId;
    private final ReadWriteLock rwLock = new MyReadWriteLock();
    private final GapLock gapLock;



    public PrimaryTuple(int id, String name, int num, PrimaryTuple next, PrimaryTuple prev, int txnId) {
        this.id = id;
        this.name = name;
        this.num = num;
        this.next = next;
        this.prev = prev;
        this.txnId = txnId;
        gapLock = new GapLock(prev, this);
    }
    private PrimaryTuple(int id, String name, int num, PrimaryTuple next, PrimaryTuple prev) {
        this(id, name, num, next, prev, -1);
    }

    public PrimaryTuple(int id) {
        this(id, null, 0, null, null, -1);
    }

    public PrimaryTuple(int id, String name, int num, int txnId) {
        this(id, name, num, null, null, txnId);
    }

    @Override
    public void setPrevVersionRecord(IDeltaStorageRecordIterator prevVersionRecord) {
        this.prevVersionRecord = prevVersionRecord;
    }
    @Override
    public IDeltaStorageRecordIterator getPrevVersionRecord() {
        return prevVersionRecord;
    }


    @Override
    public IPrimaryTuple buildOldVersion(IDeltaStorageRecordUpdater record) {
        PrimaryTuple clone = new PrimaryTuple(id, name, num, next, prev);
        record.update(clone);
        return clone;
    }

    @Override
    public boolean offsetExists(int offset) {
        return offset >= 0 && offset <= 2;
    }

    public Comparable getOffsetValue(int i) {
        if (i == 0) return id;
        else if (i == 1) return name;
        else return num;
    }

    public void setOffsetValue(int i, Comparable val, int txnId) {
        this.txnId = txnId;
        if (i != 2) {
            throw new IllegalStateException("myDB verison 0.1 not support update index column");
        }
        num = (Integer) val;
    }

    @Override
    public ReadWriteLock getRecordLock() {
        return rwLock;
    }


    @Override
    public GapLock getGapLock() {
        return gapLock;
    }

    @Override
    public ITuple prev() {
        return prev;
    }

    @Override
    public ITuple next() {
        return next;
    }

    @Override
    public String getOffsetName(int columns) {
        if (columns == 0) return "id";
        else if (columns == 1) return "name";
        else return "num";
    }


    @Override
    public int getTxnId() {
        return txnId;
    }

    @Override
    public int compareTo(PrimaryTuple o) {
        return Integer.compare(this.id, o.id);
    }

    @Override
    public boolean isPrimary() {
        return true;
    }

    @Override
    public String toString() {
        return "id=" + id +
                ", name='" + name + '\'' +
                ", num=" + num;
    }
}
