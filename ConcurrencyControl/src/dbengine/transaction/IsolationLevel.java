package dbengine.transaction;

public enum IsolationLevel {
    NO {
        @Override
        public IIsolationLevel getIIsolationLevel() {
            return new NoTransactionProtect();
        }
    },
    RU{
        @Override
        public IIsolationLevel getIIsolationLevel() {
            return new ReadUncomitted();
        }
    }, RC {
        @Override
        public IIsolationLevel getIIsolationLevel() {
            return new ReadComitted();
        }
    }, RR {
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
