package dbengine.storage;

import dbengine.storage.tables.ITable;
import dbms.SystemCatalog;
import serverlayer.model.Predicate;

public enum Expression {
    LESS_EQUAL("<=") {
        @Override
        public boolean check(Comparable a, Comparable b) {
            return a.compareTo(b) <= 0;
        }
    }, LARGER_EQUAL(">=") {
        @Override
        public boolean check(Comparable a, Comparable b) {
            return a.compareTo(b) >= 0;
        }
    },EQUAL("="){
        @Override
        public boolean check(Comparable a, Comparable b) {
            return a.compareTo(b) == 0;
        }
    }, LARGER(">") {
        @Override
        public boolean check(Comparable a, Comparable b) {
            return a.compareTo(b) > 0;
        }
    }, LESS("<") {
        @Override
        public boolean check(Comparable a, Comparable b) {
            return a.compareTo(b) < 0;
        }
    };
    private String tag;

    Expression(String tag) {
        this.tag = tag;
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
}
