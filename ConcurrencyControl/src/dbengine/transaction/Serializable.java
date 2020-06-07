//package transaction;
//
//import storage.IIndex;
//import storage.ITuple;
//import storage.clusterIndex.GapLock;
//
//public class Serializable implements IIsolationLevel {
//
//    @Override
//    public void insert(IIndex table, ITuple tuple) {
//        GapLock lock = table.findGapLock(tuple);
//        lock.lockInWriteMode();
//        ITuple ret = table.insert(tuple);
//        lock.unlockInWriteMode();
//        addLock(LockMode.EXCLUSIVE, ret);
//        addLock(LockMode.SHARE, ret.getPairGapLock());
//    }
//
//    @Override
//    public ITuple select(IIndex table, ITuple searchKey, LockMode mode) {
//        ITuple tuple;
//        tuple = table.findTuple(searchKey);
//        addLock(mode, tuple);
//        addLock(LockMode.SHARE, tuple.getPairGapLock());
//        return tuple;
//    }
//
//    @Override
//    public ITuple next(ITuple prevTuple, LockMode mode) {
//        ITuple next = prevTuple.next();
//        addLock(mode, next);
//        addLock(LockMode.SHARE, next.getPairGapLock());
//        return next;
//    }
//
//}
