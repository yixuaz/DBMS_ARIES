package serverlayer.model;

import dbengine.storage.Expression;
import dbengine.storage.clusterIndex.IPrimaryTuple;

public class UpdatedFunction extends Predicate {
    public UpdatedFunction(int offset, Comparable value) {
        super(offset, Expression.EQUAL, value);
    }

    public boolean update(IPrimaryTuple tuple, int txnId) {
        int updatedOffset = getOffset();
        Comparable oldVal = tuple.getOffsetValue(updatedOffset);
        tuple.setOffsetValue(offset, value, txnId);
        return value.compareTo(oldVal) != 0;
    }
}
