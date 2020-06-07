package dbengine.storage;

public interface IIndex {
    ITuple findTuple(ITuple searchKey);
    ITuple firstTuple();
    ITuple insert(ITuple tuple);
    ITuple endDummy();
}
