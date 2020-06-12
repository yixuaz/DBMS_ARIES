package dbengine.storage;

import dbengine.storage.multipleversion.IMultipleVersion;

import java.util.Collections;
import java.util.List;

/**
 * a tuple is a leaf node in a index, it should be comparable and have multiple version and should have lock
 *
 * @param <T>
 */
public interface ITuple<T extends ITuple> extends IDbTupleLock, IMultipleVersion, Comparable<T> {

    boolean isPrimary();

    // check this offset is exists in the tuple
    boolean offsetExists(int offset);

    // get the tuple's value from this offset
    Comparable getOffsetValue(int offset);

    // set the tuple's value from this offset
    void setOffsetValue(int offset, Comparable val, int txnId);

    // get this offset name (like 0 -> id, 1 -> name)
    String getOffsetName(int columns);

    // link to the previous leaf node
    ITuple prev();

    // link to the next leaf node
    ITuple next();

    default String toString(List<Integer> requiredColumns) {
        Collections.sort(requiredColumns);
        StringBuilder stringBuilder = new StringBuilder();
        if (!isPrimary()) {
            stringBuilder.append("[SEC_IDX]");
        }
        for (int columns : requiredColumns) {
            Comparable value = getOffsetValue(columns);
            String valueStr = value.toString();
            if (value instanceof String) {
               valueStr = "'" + valueStr + "'";
            }
            stringBuilder.append(getOffsetName(columns)).append("=").append(valueStr).append(", ");
        }
        stringBuilder.setLength(stringBuilder.length() - 2);
        return stringBuilder.toString();
    }



}
