package dbengine.storage;

import dbengine.storage.tables.ITable;
import dbms.SystemCatalog;
import serverlayer.model.Predicate;

public enum Expression {
    LESS_EQUAL("<=",0) {
        @Override
        public boolean check(Comparable a, Comparable b) {
            return a.compareTo(b) <= 0;
        }
    }, LARGER_EQUAL(">=",1) {
        @Override
        public boolean check(Comparable a, Comparable b) {
            return a.compareTo(b) >= 0;
        }
    },EQUAL("=",2){
        @Override
        public boolean check(Comparable a, Comparable b) {
            return a.compareTo(b) == 0;
        }
    }, LARGER(">",1) {
        @Override
        public boolean check(Comparable a, Comparable b) {
            return a.compareTo(b) > 0;
        }
    }, LESS("<",0) {
        @Override
        public boolean check(Comparable a, Comparable b) {
            return a.compareTo(b) < 0;
        }
    };
    private String tag;
    private int indexSelectionPriority;
    Expression(String tag, int indexSelectionPriority) {
        this.indexSelectionPriority = indexSelectionPriority;
        this.tag = tag;
    }

    public int getIndexSelectPriority() {
        return indexSelectionPriority;
    }

    public abstract boolean check(Comparable a, Comparable b);

    public static Predicate generatedPredicate(String condition, ITable table) {
        for (Expression exp : values()) {
            int sign = condition.indexOf(exp.tag);
            if (sign == -1) continue;
            String columnName = condition.substring(0, sign).trim();
            int offset = SystemCatalog.getTableConfig(table).getOffset(columnName);
            String value = condition.substring(sign + exp.tag.length(), condition.length()).trim();
            if (value.startsWith("\"") && value.endsWith("\"") ||
                    value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
                return new Predicate(offset, exp, value);
            } else {
                return new Predicate(offset, exp, Integer.parseInt(value));
            }
        }
        throw new IllegalArgumentException("invalid condition");
    }

    public static boolean isEqual(Expression p) {
        return p == EQUAL;
    }
}
