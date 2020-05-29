package dbms;

import client.TinyDBClient;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test cases, do not change this class
 */
public class TinyDBClientTest {

    private static final int CRASH_TIMES_IN_ONE_TEST = 10;
    private static final int TEST_TIMES = 50;
    /**
     * this test only have one txn and only have commit, when it do increment, there are 0.1% possibilty to crash.
     * we need to restart db, then to check the txn atomicity should not be broken.
     */
    @Test
    public void testTxnAllCommit() {
        for (int testId = 0; testId < TEST_TIMES; testId++) {
            TinyDBClient dbClient = new TinyDBClient();
            int commitCnt = 0;
            for (int i = 0; i < CRASH_TIMES_IN_ONE_TEST; i++) {
                while (true) {
                    double possibilty = 1;
                    for (int j = 0; j < 10; j++) {
                        dbClient.doit("t1-u-p" + j);
                        possibilty = Math.random();
                        if (possibilty < 0.001) { // 0.1% to crash
                            break;
                        }
                    }
                    if (possibilty < 0.001) break;
                    dbClient.doit("t1-cmt");
                    commitCnt++;
                }
                dbClient.crash();
                dbClient.start();
                String selectResult = dbClient.doit("slct");
                System.out.println(selectResult);
                Set<Integer> isAllSame = new HashSet<>();
                for (String s : selectResult.split(",")) {
                    isAllSame.add(Integer.parseInt(s.trim()));
                }
                assertEquals(1, isAllSame.size());
                assertEquals(commitCnt, isAllSame.iterator().next().intValue());
            }
        }
    }

    /**
     * this test only have one txn and  have commit(60%) and abort(40%),
     * when it do increment, there are 0.1% possibilty to crash.
     * we need to restart db, then to check the txn atomicity should not be broken.
     */
    @Test
    public void testTxnWithAbort() {
        for (int testId = 0; testId < TEST_TIMES; testId++) {
            TinyDBClient dbClient = new TinyDBClient();
            int commitCnt = 0, txnId = 0;
            for (int i = 0; i < CRASH_TIMES_IN_ONE_TEST; i++) {
                while (true) {
                    double possibilty = 1;
                    for (int j = 0; j < 10; j++) {
                        dbClient.doit("t" + txnId + "-u-p" + j);
                        possibilty = Math.random();
                        if (possibilty < 0.001) {
                            break;
                        }
                    }

                    if (possibilty < 0.001) {
                        txnId++;
                        break;
                    }

                    if (possibilty > 0.4) {
                        dbClient.doit("t" + txnId + "-cmt");
                        commitCnt++;
                    } else {
                        dbClient.doit("t" + txnId + "-abt");
                    }
                    txnId++;
                }
                dbClient.crash();
                dbClient.start();

                String selectResult = dbClient.doit("slct");
                System.out.println(selectResult);
                Set<Integer> isAllSame = new HashSet<>();
                for (String s : selectResult.split(",")) {
                    isAllSame.add(Integer.parseInt(s.trim()));
                }
                assertEquals(1, isAllSame.size());
                assertEquals(commitCnt, isAllSame.iterator().next().intValue());
            }
        }
    }

    /**
     * test steal + no force policy
     * this test only have one txn and  have commit(20%) and abort(80%),
     * when it do increment, there are 0.1% possibilty to crash and 10% to trigger flush
     * we need to restart db, then to check the txn atomicity should not be broken.
     */
    @Test
    public void testTxnWithRandomFlush() {
        for (int testId = 0; testId < TEST_TIMES; testId++) {
            TinyDBClient dbClient = new TinyDBClient();
            int commitCnt = 0, txnId = 0;
            for (int i = 0; i < CRASH_TIMES_IN_ONE_TEST; i++) {
                while (true) {
                    double possibilty = 1;
                    for (int j = 0; j < 10; j++) {
                        dbClient.doit("t" + txnId + "-u-p" + j);
                        possibilty = Math.random();
                        if (possibilty < 0.001) {
                            break;
                        }
                        if (possibilty > 0.9) {
                            dbClient.doit("flush");
                        }
                    }

                    if (possibilty < 0.001) {
                        txnId++;
                        break;
                    }

                    if (possibilty > 0.8) {
                        dbClient.doit("t" + txnId + "-cmt");
                        commitCnt++;
                    } else {
                        dbClient.doit("t" + txnId + "-abt");
                    }
                    txnId++;
                }
                dbClient.crash();
                dbClient.start();

                String selectResult = dbClient.doit("slct");
                System.out.println(selectResult);
                Set<Integer> isAllSame = new HashSet<>();
                for (String s : selectResult.split(",")) {
                    isAllSame.add(Integer.parseInt(s.trim()));
                }
                assertEquals(1, isAllSame.size());
                assertEquals(commitCnt, isAllSame.iterator().next().intValue());
            }
        }
    }

    /**
     * test stop-the-world check point
     * this test only have one txn and  have commit(60%) and abort(40%),
     * when it do increment, there are 0.1% possibilty to crash and 1% to trigger flush
     * and 4% to do a check point
     * we need to restart db, then to check the txn atomicity should not be broken.
     */
    @Test
    public void testTxnWithRandomFlushAndNormalCP() {
        for (int testId = 0; testId < TEST_TIMES; testId++) {
            TinyDBClient dbClient = new TinyDBClient();
            int commitCnt = 0, txnId = 0;
            for (int i = 0; i < CRASH_TIMES_IN_ONE_TEST; i++) {
                while (true) {
                    double possibilty = 1;
                    for (int j = 0; j < 10; j++) {
                        dbClient.doit("t" + txnId + "-u-p" + j);
                        possibilty = Math.random();
                        if (possibilty < 0.001) {
                            break;
                        }
                        if (possibilty > 0.99) {
                            dbClient.doit("flush");
                        } else if (possibilty > 0.95) {
                            dbClient.doit("cp-st");
                            dbClient.doit("cp-ed");
                        }
                    }

                    if (possibilty < 0.001) {
                        txnId++;
                        break;
                    }
                    if (possibilty > 0.4) {
                        dbClient.doit("t" + txnId + "-cmt");
                        commitCnt++;
                    } else {
                        dbClient.doit("t" + txnId + "-abt");
                    }
                    txnId++;
                }
                dbClient.crash();
                dbClient.start();

                String selectResult = dbClient.doit("slct");
                System.out.println(selectResult);
                Set<Integer> isAllSame = new HashSet<>();
                for (String s : selectResult.split(",")) {
                    isAllSame.add(Integer.parseInt(s.trim()));
                }
                assertEquals(dbClient.doit("show-log"), 1, isAllSame.size());
                assertEquals(commitCnt, isAllSame.iterator().next().intValue());
            }
        }
    }

    /**
     * test fuzzy check point
     * this test only have one txn and  have commit(60%) and abort(40%),
     * when it do increment, there are 0.1% possibilty to crash and 1% to trigger flush
     * we need to restart db, then to check the txn atomicity should not be broken.
     */
    @Test
    public void testTxnWithRandomFlushAndFuzzyCP() {
        for (int testId = 0; testId < TEST_TIMES; testId++) {
            TinyDBClient dbClient = new TinyDBClient();
            int commitCnt = 0, txnId = 0;
            boolean isCheckPointStarted = false;
            for (int i = 0; i < CRASH_TIMES_IN_ONE_TEST; i++) {
                while (true) {
                    double possibilty = 1;
                    for (int j = 0; j < 10; j++) {
                        dbClient.doit("t" + txnId + "-u-p" + j);
                        possibilty = Math.random();
                        if (possibilty < 0.001) {
                            break;
                        }
                        if (possibilty > 0.99) {
                            dbClient.doit("flush");
                        } else if (!isCheckPointStarted && possibilty > 0.95) {
                            dbClient.doit("cp-st");
                            isCheckPointStarted = !isCheckPointStarted;
                        } else if (isCheckPointStarted && possibilty > 0.8) {
                            dbClient.doit("cp-ed");
                            isCheckPointStarted = !isCheckPointStarted;
                        }
                    }

                    if (possibilty < 0.001) {
                        txnId++;
                        break;
                    }
                    if (possibilty > 0.4) {
                        dbClient.doit("t" + txnId + "-cmt");
                        commitCnt++;
                    } else {
                        dbClient.doit("t" + txnId + "-abt");
                    }
                    txnId++;
                }
                dbClient.crash();
                dbClient.start();

                String selectResult = dbClient.doit("slct");
                System.out.println(selectResult);
                Set<Integer> isAllSame = new HashSet<>();
                for (String s : selectResult.split(",")) {
                    isAllSame.add(Integer.parseInt(s.trim()));
                }
                assertEquals(dbClient.doit("show-log"), 1, isAllSame.size());
                assertEquals(commitCnt, isAllSame.iterator().next().intValue());
            }
        }
    }

    /**
     * test 1970 dbms, easily to crash
     * this test only have one txn and  have commit(60%) and abort(40%),
     * when it do increment, there are 10% possibilty to crash and 1% to trigger flush
     * we need to restart db, then to check the txn atomicity should not be broken.
     */
    @Test
    public void testTxnDBEasyCrash() {
        for (int testId = 0; testId < TEST_TIMES; testId++) {
            TinyDBClient dbClient = new TinyDBClient();
            int commitCnt = 0, txnId = 0;
            boolean isCheckPointStarted = false;
            for (int i = 0; i < CRASH_TIMES_IN_ONE_TEST; i++) {
                while (true) {
                    double possibilty = 1;
                    for (int j = 0; j < 10; j++) {
                        dbClient.doit("t" + txnId + "-u-p" + j);
                        possibilty = Math.random();
                        if (possibilty < 0.1) { // 10% possibility to crash for each action
                            break;
                        }
                        if (possibilty > 0.99) {
                            dbClient.doit("flush");
                        } else if (!isCheckPointStarted && possibilty > 0.98) {
                            dbClient.doit("cp-st");
                            isCheckPointStarted = !isCheckPointStarted;
                        } else if (isCheckPointStarted && possibilty > 0.5) {
                            dbClient.doit("cp-ed");
                            isCheckPointStarted = !isCheckPointStarted;
                        }
                    }

                    if (possibilty < 0.1) {
                        txnId++;
                        break;
                    }
                    if (possibilty > 0.4) {
                        dbClient.doit("t" + txnId + "-cmt");
                        commitCnt++;
                    } else {
                        dbClient.doit("t" + txnId + "-abt");
                    }
                    txnId++;
                }
                dbClient.crash();
                dbClient.start();

                String selectResult = dbClient.doit("slct");
                System.out.println(selectResult);
                Set<Integer> isAllSame = new HashSet<>();
                for (String s : selectResult.split(",")) {
                    isAllSame.add(Integer.parseInt(s.trim()));
                }
                assertEquals(dbClient.doit("show-log"), 1, isAllSame.size());
                assertEquals(commitCnt, isAllSame.iterator().next().intValue());
            }
        }
    }

    /**
     * test fuzzy check point
     * this test only have one txn and  have commit(60%) and abort(40%),
     * when it do increment, there are 0.1% possibilty to crash and 1% to trigger flush
     * we need to restart db, then to check the txn atomicity should not be broken.
     */
    @Test
    public void testRecoverProcessAlsoCouldCrash() {
        for (int testId = 0; testId < TEST_TIMES; testId++) {
            TinyDBClient dbClient = new TinyDBClient();
            int commitCnt = 0, txnId = 0;
            boolean isCheckPointStarted = false;
            boolean recoverFailed = false;
            for (int i = 0; i < CRASH_TIMES_IN_ONE_TEST; i++) {
                while (true) {
                    double possibilty = 1;
                    for (int j = 0; j < 10; j++) {
                        dbClient.doit("t" + txnId + "-u-p" + j);
                        possibilty = Math.random();
                        if (possibilty < 0.001) {
                            break;
                        }
                        if (possibilty > 0.99) {
                            dbClient.doit("flush");
                        } else if (!isCheckPointStarted && possibilty > 0.98) {
                            dbClient.doit("cp-st");
                            isCheckPointStarted = !isCheckPointStarted;
                        } else if (isCheckPointStarted && possibilty > 0.8) {
                            dbClient.doit("cp-ed");
                            isCheckPointStarted = !isCheckPointStarted;
                        }
                    }

                    if (possibilty < 0.001) {
                        txnId++;
                        break;
                    }
                    if (possibilty > 0.4) {
                        dbClient.doit("t" + txnId + "-cmt");
                        commitCnt++;
                    } else {
                        dbClient.doit("t" + txnId + "-abt");
                    }
                    txnId++;
                }
                dbClient.crash();
                while (!dbClient.start(0.25)) recoverFailed = true;

                String selectResult = dbClient.doit("slct");
                System.out.println(selectResult);
                Set<Integer> isAllSame = new HashSet<>();
                for (String s : selectResult.split(",")) {
                    isAllSame.add(Integer.parseInt(s.trim()));
                }
                assertEquals(dbClient.doit("show-log"), 1, isAllSame.size());
                assertEquals(commitCnt, isAllSame.iterator().next().intValue());
            }
            assertTrue(recoverFailed);
        }
    }
}