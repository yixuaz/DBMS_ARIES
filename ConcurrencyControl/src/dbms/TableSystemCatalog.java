package dbms;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TableSystemCatalog {
    // hard code for myTable
    private Map<String, Integer> column2Offset = new HashMap<>();
    private Map<Integer, Converter> converters = new HashMap<>();
    private int primaryIndexOffset = 0;

    interface Converter {
        Comparable convert(String s);
    }
    private Set<Integer> otherIndexsOffsets = new HashSet<>();
    {
        column2Offset.put("id", 0);
        column2Offset.put("name", 1);
        column2Offset.put("num", 2);
        converters.put(0, (s)->Integer.parseInt(s));
        converters.put(1, (s)->s);
        converters.put(2, (s)->Integer.parseInt(s));
        otherIndexsOffsets.add(1);
    }

    public int getOffset(String name) {
        return column2Offset.get(name);
    }

    public  boolean containsSecondaryIndex(int offset) {
        return otherIndexsOffsets.contains(offset);
    }

    public Comparable toRealType(int offset, String val) {
        return converters.get(offset).convert(val);
    }
}
