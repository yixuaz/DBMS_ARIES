import dbengine.transaction.IsolationLevel;
import dbms.SystemCatalog;
import dbms.TransactionThread;
import model.SqlMsg;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class BaseTest {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    abstract public IsolationLevel getIsolationLevel();

    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
        SystemCatalog.reset();
    }

    @After
    public void restoreStreams() {
        System.setOut(originalOut);
    }

    protected void testTemplate(int txnNums, List<SqlMsg> msgs, List<String> assertContents) throws InterruptedException, IOException, ExecutionException {
        List<BlockingQueue<String>> txns = new ArrayList<>();
        List<FutureTask> futureTasks = new ArrayList<>();
        for (int i = 0; i < txnNums; i++) {
            txns.add(new LinkedBlockingQueue<>());
            futureTasks.add(new FutureTask<Void>(
                    new TransactionThread(getIsolationLevel(), txns.get(i))));
            new Thread(futureTasks.get(futureTasks.size() - 1)).start();
        }
        for (SqlMsg sql : msgs) {
            txns.get(sql.getTxnId()).add(sql.getSql());
            Thread.sleep(110);
        }
        for (FutureTask ft : futureTasks) ft.get();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                new ByteArrayInputStream(outContent.toByteArray())))) {
            int idx = 0;
            for (String assertContent : assertContents) {
                Assert.assertEquals(idx + "", assertContent, br.readLine());
                idx++;
            }
            Assert.assertTrue(br.readLine() == null);
        }
    }
}
