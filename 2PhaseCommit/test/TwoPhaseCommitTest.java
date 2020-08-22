import client.Client;
import org.junit.Test;
import server.Node;
import server.Participant;
import server.TinyDbShardingNode;
import server.TxnCoordinator;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TwoPhaseCommitTest {
    private static final int TEST_TIMES = 50;
    @Test
    public void testAtomicityWithoutAbort() {
        for (int testId = 0; testId < TEST_TIMES; testId++) {
            TxnCoordinator txnCoordinator = new TxnCoordinator(50, 20, 0);
            Client client = new Client(txnCoordinator, 100);
            int txnTimes = 500;
            int successTimes = 0, timeOutTimes = 0;
            for (int i = 0; i < txnTimes; i++) {
                if (client.updateInTxn(0, 19)) {
                    successTimes++;
                } else timeOutTimes++;
            }
            System.out.println("timeout : " + timeOutTimes);
            assertTrue(successTimes + "", successTimes == txnTimes - timeOutTimes);
            checkAtomicity(client, successTimes);
        }
    }

    @Test
    public void testAtomicityWithAbort() {
        for (int testId = 0; testId < TEST_TIMES; testId++) {
            for (int pageNum = 20; pageNum <= 50; pageNum += 10) {
                TxnCoordinator txnCoordinator = new TxnCoordinator(100, pageNum, 0.05);
                Client client = new Client(txnCoordinator, 100);
                int txnTimes = 500;
                int successTimes = 0;
                for (int i = 0; i < txnTimes; i++) {
                    if (client.updateInTxn(0, pageNum - 1)) {
                        successTimes++;
                    }
                }
                assertTrue(successTimes < txnTimes);
                checkAtomicity(client, successTimes);
            }
        }
    }

    @Test
    public void testAtomicityWithParticipantCrashOutside2PC() {
        for (int testId = 0; testId < TEST_TIMES; testId++) {
            int txnTimes = 100, pageNum = 20;
            TxnCoordinator txnCoordinator = new TxnCoordinator(10, pageNum, 0.05);
            Client client = new Client(txnCoordinator, 20);
            List<Participant> participantList = txnCoordinator.getParticipants();
            int successTimes = 0, participantsSize = participantList.size();
            int shutDownParticipant = -1;
            Random random = new Random();
            for (int i = 0; i < txnTimes; i++) {
                boolean updateSuccess = client.updateInTxn(0, pageNum - 1);
                if (updateSuccess) {
                    successTimes++;
                }
                if (Math.random() < 0.05 && shutDownParticipant == -1) {
                    shutDownParticipant = random.nextInt(participantsSize);
                    participantList.get(shutDownParticipant).shutDown();
                } else if (shutDownParticipant != -1 && Math.random() < 0.5) {
                    participantList.get(shutDownParticipant).reboot();
                    shutDownParticipant = -1;
                } else if (shutDownParticipant == -1 && Math.random() < 0.3) {
                    checkAtomicity(client, successTimes);
                }
            }
            assertTrue(successTimes <= txnTimes);
            if (shutDownParticipant != -1) {
                String queryResult = client.queryAllShardDbPages();
                assertEquals("TIME OUT", queryResult);
            } else {
                checkAtomicity(client, successTimes);
            }
        }

    }

    @Test
    public void testAtomicityWithOneParticipantReplyYesThenCrashNeedCommitAfterReboot() {
        for (int testId = 0; testId < TEST_TIMES; testId++) {
            int pageNum = 20;
            TxnCoordinator txnCoordinator = new TxnCoordinator(50, pageNum);
            txnCoordinator.breakPointBeforeRecordVoteResult.add(0);
            Client client = new Client(txnCoordinator, 60);
            client.updateInTxn(0, pageNum - 1);
            assertFalse(client.updateInTxn(0, pageNum - 1));
            assertEquals("TIME OUT", client.queryAllShardDbPages());
            txnCoordinator.getParticipants().get(0).reboot();
            checkAtomicity(client, 1);
            client.updateInTxn(0, pageNum - 1);
            checkAtomicity(client, 2);
        }
    }

    @Test
    public void testAtomicityWithOneParticipantReplyYesThenCrashNeedAbortAfterReboot() {
        int pageNum = 20;
        TxnCoordinator txnCoordinator = new TxnCoordinator(50, pageNum, 0, 1);
        txnCoordinator.breakPointBeforeRecordVoteResult.add(0); // 0 means participant 0 crash
        Client client = new Client(txnCoordinator, 100);
        client.updateInTxn(0, pageNum - 1);
        assertFalse(client.updateInTxn(0, pageNum - 1));
        assertEquals("TIME OUT", client.queryAllShardDbPages());
        txnCoordinator.getParticipants().get(0).reboot();
        checkAtomicity(client, 0);
        client.updateInTxn(0, pageNum - 1);
        checkAtomicity(client, 0);
    }

    @Test
    public void testAtomicityWithOneParticipantReplyYesThenTCCrash() {
        int pageNum = 20;
        TxnCoordinator txnCoordinator = new TxnCoordinator(50, pageNum);
        txnCoordinator.breakPointBeforeRecordVoteResult.add(-1); // -1 means tc crash
        Client client = new Client(txnCoordinator, 100);
        client.updateInTxn(0, pageNum - 1);
        assertFalse(client.updateInTxn(0, pageNum - 1));
        assertEquals("TIME OUT", client.queryAllShardDbPages());
        txnCoordinator.reboot();
        checkAtomicity(client, 0);
        client.updateInTxn(0, pageNum - 1);
        checkAtomicity(client, 1);
    }

    @Test
    public void testAtomicityWithReplyYesThenTCCrashWillWaitUntilTCReboot() {
        int pageNum = 20;
        TxnCoordinator txnCoordinator = new TxnCoordinator(50, pageNum);
        txnCoordinator.breakPointBeforeRecordVoteResult.add(-1); // -1 means tc crash
        Client client = new Client(txnCoordinator, 100);
        client.updateInTxn(0, pageNum - 1);
        assertFalse(client.updateInTxn(0, pageNum - 1));
        assertEquals("TIME OUT", client.queryAllShardDbPages());
        assertEquals(true, txnCoordinator.isTxnNotFinished(1));
        checkNodesWaitTxnFinshed(1, txnCoordinator, false);
        txnCoordinator.reboot();
        checkAtomicity(client, 0);
        for (Participant participant : txnCoordinator.getParticipants()) {
            assertEquals(false, ((Node) participant).isTxnNotFinished(1));
        }
        client.updateInTxn(0, pageNum - 1);
        checkAtomicity(client, 1);
    }

    private void checkNodesWaitTxnFinshed(int txnID, TxnCoordinator txnCoordinator, boolean finshed) {
        for (Participant participant : txnCoordinator.getParticipants()) {
            assertEquals(!finshed, ((Node) participant).isTxnNotFinished(1));
        }
    }

    @Test
    public void testAtomicityWithReplyNoThenTCCrashWillNoWait() {
        int pageNum = 20;
        TxnCoordinator txnCoordinator = new TxnCoordinator(50, pageNum, 1);
        txnCoordinator.breakPointBeforeRecordVoteResult.add(-1); // -1 means tc crash
        Client client = new Client(txnCoordinator, 100);
        client.updateInTxn(0, pageNum - 1);
        assertFalse(client.updateInTxn(0, pageNum - 1));
        assertEquals("TIME OUT", client.queryAllShardDbPages());
        assertEquals(true, txnCoordinator.isTxnNotFinished(1));
        checkNodesWaitTxnFinshed(1, txnCoordinator, true);
        txnCoordinator.reboot();
        checkAtomicity(client, 0);
    }

    @Test
    public void testAtomicityWithReplyYesThenTCCrash2() {
        int pageNum = 20;
        TxnCoordinator txnCoordinator = new TxnCoordinator(50, pageNum);
        txnCoordinator.breakPointAfterRecordVoteResult.add(-1); // -1 means tc crash
        Client client = new Client(txnCoordinator, 100);
        client.updateInTxn(0, pageNum - 1);
        assertFalse(client.updateInTxn(0, pageNum - 1));
        assertEquals("TIME OUT", client.queryAllShardDbPages());
        assertEquals(false, txnCoordinator.isTxnNotFinished(1));
        checkNodesWaitTxnFinshed(1, txnCoordinator, false);
        txnCoordinator.reboot();
        checkAtomicity(client, 1);
    }

    @Test
    public void testAtomicityWithCrashRandomly() {
        for (int testId = 0; testId < TEST_TIMES; testId++) {
            int pageNum = 20, txnTimes = 20;
            TxnCoordinator txnCoordinator = new TxnCoordinator(5, pageNum, 0);
            Client client = new Client(txnCoordinator, 10);
            Random r = new Random();
            int status = 0;
            for (int k = 0; k < 50; k++) {
                for (int i = 1; i <= txnTimes; i++) {
                    client.updateInTxn(0, pageNum - 1);
                    if (Math.random() < 0.2) {
                        if (Math.random() < 0.5 && status <= 0) {
                            status = flipStatus(txnCoordinator);
                        } else if (status >= 0) {
                            List<Participant> participants = txnCoordinator.getParticipants();
                            int id = 1 + r.nextInt((pageNum + 9) / 10);
                            Participant randomParticipant = participants.get(id - 1);
                            status += flipStatus(randomParticipant, id);
                        }
                    }
                }
                if (status == 0) {
                    System.out.println("here");
                    checkAtomicity(client, txnCoordinator.successTimes());
                }
            }
        }
    }

    private void checkAtomicity(Client client, int successTime) {
        Set<Integer> isAllSame = new HashSet<>();
        client.waitAllNodesCompleteProcessingTask();
        String res;
        do {
            assertTrue(!client.getTxnCoordinator().isShutDown());
            res = client.queryAllShardDbPages();
        } while (TxnCoordinator.TIMEOUT_RESP.equals(res));
        for (String s : res.split(",")) {
            isAllSame.add(Integer.parseInt(s.trim()));
        }
        assertEquals(res, 1, isAllSame.size());
        assertEquals(successTimeForEachPart(client.getTxnCoordinator().getParticipants()),
                client.getTxnCoordinator().successTimes(), isAllSame.iterator().next().intValue());
    }

    private String successTimeForEachPart(List<Participant> participants) {
        String s = "";
        for (Participant p : participants) {
            s += ((TinyDbShardingNode) p).successTimes() + ",";
        }
        return s;
    }

    private int flipStatus(TxnCoordinator txnCoordinator) {
        if (txnCoordinator.isShutDown()) {
            txnCoordinator.reboot();
            return 0;
        } else {
            if (Math.random() < 0.5 && txnCoordinator.breakPointAfterRecordVoteResult.isEmpty()) {
                txnCoordinator.breakPointAfterRecordVoteResult.add(-1);
                return -1;
            }
            if (txnCoordinator.breakPointAfterRecordVoteResult.isEmpty()) {
                txnCoordinator.breakPointBeforeRecordVoteResult.add(-1);
            }
            return -1;
        }
    }

    private int flipStatus(Participant participant, int id) {
        if (participant.isShutDown()) {
            participant.reboot();
            return -id;
        }
        participant.shutDown();
        return id;
    }

}
