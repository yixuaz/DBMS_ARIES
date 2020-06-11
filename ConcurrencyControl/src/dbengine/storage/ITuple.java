package dbengine.storage;

import dbengine.storage.multipleversion.IMultipleVersion;

import java.util.Collections;
import java.util.List;

public interface ITuple<T extends ITuple> extends IDBLock, IMultipleVersion, Comparable<T> {

    boolean isPrimary();

    boolean isUnique();

    String toString();

    boolean haveOffsetValue(int offset);

    Comparable getOffsetValue(int offset) ;

    void setOffsetValue(int offset, Comparable val, int txnId) ;

    ITuple prev();

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

    String getOffsetName(int columns);


}
