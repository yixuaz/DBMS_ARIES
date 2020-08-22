package server;

import java.util.List;
import java.util.concurrent.Future;

public interface Participant {
    /**
     * 2PC phase 1, do some job to update db then flush log before reply yes to avoid reply commit but data lost
     * @param txnId
     * @param incrementPages
     * @return
     */
    Future<Boolean> canCommit(int txnId, List<Integer> incrementPages);

    /**
     * 2PC phase 2, tc send commit this transaction id
     * @param txnId
     * @return
     */
    Future<Boolean> doCommit(int txnId);

    /**
     * 2PC phase 2, tc send abort this transaction id
     * @param txnId
     * @return
     */
    Future<Boolean> doAbort(int txnId);

    Future<String> selectResult();
    void shutDown();
    void reboot();
    void resolveUnfinishedTask();
    boolean isShutDown();
}
