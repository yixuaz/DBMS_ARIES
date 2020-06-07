package serverlayer.model;

import dbengine.storage.Expression;
import dbengine.storage.ITuple;

public class Predicate {
    int offset;
    Expression expression;
    Comparable value;

    public Predicate(int offset, Expression expression, Comparable value) {
        this.offset = offset;
        this.expression = expression;
        this.value = value;
    }

    public boolean check(Comparable other) {
        return expression.check(other, value);
    }

    public int getOffset() {
        return offset;
    }

    public Expression getExpression() {
        return expression;
    }

    public Comparable getValue() {
        return value;
    }

    public boolean check(ITuple other) {
        return expression.check(other.getOffsetValue(offset), value);
    }
}
