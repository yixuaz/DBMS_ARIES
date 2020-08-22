package server;

import server.model.TxnResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;

import static util.FutureTimeLimitGetter.generateFutureResultListOrDefault;

public class TxnCoordinator extends Node {
    // range sharding: page 0~9 -> Participant1, 10~19 -> Participant2 ...
    private final List<Participant> participants;
    private final long timeoutThreshold;
    private int curTxnId = 0;
    private final ConcurrentHashMap<Integer, Boolean> permantStorage; // txnId -> result

    // below is for testing
    public final ConcurrentLinkedDeque<Integer> breakPointBeforeRecordVoteResult = new ConcurrentLinkedDeque<>();
    public final ConcurrentLinkedDeque<Integer> breakPointAfterRecordVoteResult = new ConcurrentLinkedDeque<>();

    public TxnCoordinator(long timeoutThreshold, int pageNums, double... abortProbability) {
        this.timeoutThreshold = timeoutThreshold;
        List<Participant> tmp = new ArrayList<>();
        int idx = 0;
        for (int i = 0; i <= (pageNums - 1) / 10; i++) {
            tmp.add(new TinyDbShardingNode(getAbortProbability(abortProbability, idx++), this));
        }
        participants = Collections.unmodifiableList(tmp);
        permantStorage = new ConcurrentHashMap<>();
    }


    public Future<List<String>> selectAll() {
        return doAsync(() -> {
            List<Future<String>> participantSelectResults = new ArrayList<>();
            for (Participant participant : participants) {
                participantSelectResults.add(participant.selectResult());
            }
            List<String> res = generateFutureResultListOrDefault(participantSelectResults, null, timeoutThreshold);
            for (String s : res) {
                if (s == null) {
                    // if one timeout, ret the timeout
                    return Arrays.asList(TIMEOUT_RESP);
                }
            }
            return res;
        });
    }

    public Future<Boolean> runDistributedTransaction(List<Integer> incrementPageIds) {
        // step 1. prepare which page send to which node from all the pages need change in one txn
        // step 2. send them with canCommit, and collect feedback
        // step 3. according the vote result, record the decision in permant storage and send doAbort or doCommit
        return doAsync(() -> {
            synchronized (this) {
                // this line to avoid concurrency txn which not support in tinyDB yet
                if (!setProcessingJob(curTxnId + 1))
                    return false;

                Map<Integer, List<Integer>> cmds = prepareCommands(incrementPageIds);
                curTxnId++;

                // phase 1 voting
                boolean voteResult = vote(cmds, curTxnId);

                shutDownServerIfNeed(breakPointBeforeRecordVoteResult);
                if (waitRebootIfShutDown()) return false;

                permantStorage.put(curTxnId, voteResult);
                boolean resetSuccess = resetProcessingJob(curTxnId);
                assert resetSuccess;

                shutDownServerIfNeed(breakPointAfterRecordVoteResult);
                if (waitRebootIfShutDown()) return false;

                // phase 2 commiting
                commit(cmds.keySet(), voteResult, curTxnId);

                return voteResult;
            }
        });
    }

    @Override
    public void reboot() {
        super.reboot();
        breakPointBeforeRecordVoteResult.clear();
        breakPointAfterRecordVoteResult.clear();
        for (Participant p : participants) p.canCommit(-1, Collections.emptyList());
    }


    private void shutDownServerIfNeed(ConcurrentLinkedDeque<Integer> breakPoint) {
        while (!breakPoint.isEmpty()) {
            int pid = breakPoint.poll();
            if (pid == -1) shutDown();
            else participants.get(pid).shutDown();
        }
    }

    public List<Participant> getParticipants() {
        return participants;
    }

    private void commit(Set<Integer> participantIds, boolean voteResult, int txnId) {
        List<Future<Boolean>> results = new ArrayList<>();
        if (voteResult) {
            for (int participantId : participantIds)
                results.add(participants.get(participantId).doCommit(txnId));
        } else {
            for (int participantId : participantIds)
                results.add(participants.get(participantId).doAbort(txnId));
        }
        generateFutureResultListOrDefault(results, false, timeoutThreshold);
    }

    public Future<TxnResult> query(final int txnId) {
        return doAsync(() -> {
            synchronized (this) {
                Boolean res = permantStorage.get(txnId);
                if (res == null) {
                    permantStorage.put(txnId, false);
                    res = false;
                }
                return res.booleanValue() ? TxnResult.COMMIT : TxnResult.ABORT;
            }
        });
    }

    private boolean vote(Map<Integer, List<Integer>> cmds, int curTxnId) {
        List<Future<Boolean>> results = new ArrayList<>();
        for (Map.Entry<Integer, List<Integer>> pid2IncrementPageIds : cmds.entrySet()) {
            int participantId = pid2IncrementPageIds.getKey();
            List<Integer> incPages = pid2IncrementPageIds.getValue();
            results.add(participants.get(participantId).canCommit(curTxnId, incPages));
        }
        boolean isAllCanCommit = true;
        for (boolean canCommit : generateFutureResultListOrDefault(results, false, timeoutThreshold)) {
            isAllCanCommit &= canCommit;
        }
        return isAllCanCommit;
    }

    private double getAbortProbability(double[] abortProbability, int i) {
        int l = abortProbability.length;
        if (i >= abortProbability.length)
            return l == 0 ? 0 : abortProbability[l - 1];
        return abortProbability[i];
    }

    private Map<Integer, List<Integer>> prepareCommands(List<Integer> incrementPageIds) {
        Map<Integer, List<Integer>> cmds = new HashMap<>();
        for (int pageId : incrementPageIds) {
            int participantId = pageId / 10;
            cmds.putIfAbsent(participantId, new ArrayList<>());
            cmds.get(participantId).add(pageId % 10);
        }
        return cmds;
    }

    @Override
    public synchronized int successTimes() {
        int successTimes = 0;
        for (boolean success : permantStorage.values()) {
            successTimes += (success ? 1 : 0);
        }
        return successTimes;
    }

    @Override
    public boolean isTxnNotFinished(int txnId) {
        return processingTxnId.get() == txnId;
    }
}
