package dbengine.transaction;

public enum IsolationLevel {
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
            return null;
        }
    };
    public abstract IIsolationLevel getIIsolationLevel();
}
