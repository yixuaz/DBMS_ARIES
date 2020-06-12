package serverlayer.model;

import dbengine.storage.GapLock;
import dbengine.storage.ITuple;

import java.util.concurrent.locks.ReadWriteLock;

public class DummyTuple implements ITuple {
    private final ITuple next;
    public DummyTuple(ITuple nextTuple) {
        next = nextTuple;
    }

    @Override
    public ITuple next() {
        return next;
    }

    @Override
    public String getOffsetName(int columns) {
        return null;
    }

    // below is useless

    @Override
    public boolean isPrimary() {
        return false;
    }

    @Override
    public boolean offsetExists(int offset) {
        return false;
    }

    @Override
    public Comparable getOffsetValue(int offset) {
        return null;
    }

    @Override
    public void setOffsetValue(int offset, Comparable val, int txnId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITuple prev() {
        return null;
    }


    @Override
    public ReadWriteLock getRecordLock() {
        return null;
    }

    @Override
    public GapLock getGapLock() {
        return null;
    }

    @Override
    public int getTxnId() {
        return 0;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
