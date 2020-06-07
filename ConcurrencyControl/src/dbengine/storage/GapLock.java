package dbengine.storage;

import dbengine.storage.clusterIndex.PrimaryTuple;
import dbengine.storage.nonclusterIndex.NonUniqueIndexTuple;

public class GapLock {
    Comparable lowBound;
    Comparable highBound;
    final Comparable MINIMUM = (other) -> -1;
    final Comparable MAXIMUM = (other) -> 1;

    public GapLock(Comparable lowBound, Comparable highBound) {
        this.lowBound = (lowBound == null) ?  MINIMUM : lowBound;
        this.highBound = (highBound == null) ?  MAXIMUM : highBound;
    }

    public boolean inGap(Comparable input) {
        return input.compareTo(lowBound) > 0 && input.compareTo(highBound) < 0;
    }

    @Override
    public String toString() {
        return "GAP(" + lowBound()
                + "," + highBound() + ")";
    }
    private String lowBound() {

        if (lowBound instanceof NonUniqueIndexTuple) {return ((NonUniqueIndexTuple) lowBound).getOffsetValue(1).toString()
                + ":" + ((ITuple) lowBound).getOffsetValue(0).toString();}
        else if (lowBound instanceof PrimaryTuple) return ((ITuple) lowBound).getOffsetValue(0).toString();
        else return "-inf";
    }
    private String highBound() {
        if (highBound instanceof NonUniqueIndexTuple){
            return ((NonUniqueIndexTuple) highBound).getOffsetValue(1).toString() + ":" + ((ITuple) highBound).getOffsetValue(0).toString();
        }
        else if (highBound instanceof PrimaryTuple) return ((ITuple) highBound).getOffsetValue(0).toString();
        else return "inf";
    }
}
