package dbengine.transaction.model;

import dbengine.transaction.IIsolationLevel;
import dbengine.transaction.NoTransactionProtect;
import dbengine.transaction.ReadComitted;
import dbengine.transaction.ReadUncomitted;
import dbengine.transaction.RepeatableRead;
import dbengine.transaction.Serializable;

/**
 * a IIsolationLevel factory class
 */
public enum IsolationLevelType {
    NO { // dirty write, dirty read, read skew, write skew, phantom read
        @Override
        public IIsolationLevel getIIsolationLevel() {
            return new NoTransactionProtect();
        }
    },
    RU { // dirty read, read skew, write skew, phantom read
        @Override
        public IIsolationLevel getIIsolationLevel() {
            return new ReadUncomitted();
        }
    }, RC { // read skew, write skew, phantom read
        @Override
        public IIsolationLevel getIIsolationLevel() {
            return new ReadComitted();
        }
    }, RR { // write skew
        @Override
        public IIsolationLevel getIIsolationLevel() {
            return new RepeatableRead();
        }
    }, SERIAL {
        @Override
        public IIsolationLevel getIIsolationLevel() {
            return new Serializable();
        }
    };

    public abstract IIsolationLevel getIIsolationLevel();
}
