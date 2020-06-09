package serverlayer.model;

import dbengine.storage.ITuple;

public class IndexSearchResult {
    final boolean targetIsTreeSearchWithEqualExp;
    final boolean lastTupleIsInvalid;
    final ITuple nextTuple;

    public IndexSearchResult(boolean targetIsTreeSearchWithEqualExp, boolean lastTupleIsInvalid, ITuple nextTuple) {
        this.targetIsTreeSearchWithEqualExp = targetIsTreeSearchWithEqualExp;
        this.lastTupleIsInvalid = lastTupleIsInvalid;
        this.nextTuple = nextTuple;
    }
}
