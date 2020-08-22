package client;

import server.Node;
import server.Participant;
import server.TxnCoordinator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static util.FutureTimeLimitGetter.getFutureResultOrDefault;

public class Client {
    private final TxnCoordinator txnCoordinator;
    private final long readTimeout;

    public Client(TxnCoordinator txnCoordinator, long readTimeout) {
        if (txnCoordinator == null || readTimeout < 0) {
            throw new IllegalArgumentException("invalid param in Client construction");
        }
        this.txnCoordinator = txnCoordinator;
        this.readTimeout = readTimeout;
    }

    public String queryAllShardDbPages() {
        String raw = getFutureResultOrDefault(txnCoordinator.selectAll(),
                Arrays.asList(Node.TIMEOUT_RESP), readTimeout).toString();
        assert raw.length() >= 2;
        return raw.substring(1, raw.length() - 1);
    }

    public boolean updateInTxn(int fromPage, int toPage) {
        List<Integer> pages = new ArrayList<>();
        for (int pid = fromPage; pid <= toPage; pid++) {
            pages.add(pid);
        }
        return updateInTxn(pages);
    }

    public boolean updateInTxn(List<Integer> updatedDataPages) {
        return getFutureResultOrDefault(txnCoordinator.runDistributedTransaction(updatedDataPages),
                false, readTimeout);
    }


    // called by test when every server is healthy
    public void waitAllNodesCompleteProcessingTask() {
        // check coordinator finished task
        while (!txnCoordinator.hasNoRunningJobs()) {
            Thread.yield();
        }
        // check each participant finished task and resolve unfinished task
        for (Participant p : txnCoordinator.getParticipants()) {
            while (!((Node) p).hasNoRunningJobs()) {
                Thread.yield();
            }
            p.resolveUnfinishedTask();
        }
    }

    public TxnCoordinator getTxnCoordinator() {
        return txnCoordinator;
    }
}
