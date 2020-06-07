package dbengine.transaction;

import dbengine.storage.ITuple;

public class LockStrategy {
    boolean isTreeSearch, meetCondition;

    public LockStrategy(boolean isTreeSearch, boolean meetCondition) {
        this.isTreeSearch = isTreeSearch;
        this.meetCondition = meetCondition;
    }
}
