package isolationlevel;

import dbengine.transaction.IIsolationLevel;
import dbengine.transaction.model.IsolationLevelType;
import dbms.DBClient;
import dbms.SystemCatalog;
import dbms.TransactionThread;
import isolationlevel.model.SqlMsg;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

    abstract public IsolationLevelType getIsolationLevel();

    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
        SystemCatalog.reset();
    }

    @After
    public void restoreStreams() {
        System.setOut(originalOut);
    }

    protected void concurrentTestTemplate(int txnNums, List<SqlMsg> msgs, List<String> assertContents) throws InterruptedException, IOException, ExecutionException {
        List<BlockingQueue<String>> txns = new ArrayList<>();
        List<FutureTask> futureTasks = new ArrayList<>();
        for (int i = 0; i < txnNums; i++) {
            txns.add(new LinkedBlockingQueue<>());
            futureTasks.add(new FutureTask<Void>(
                    new TransactionThread(getIsolationLevel(), txns.get(i))));
            new Thread(futureTasks.get(futureTasks.size() - 1)).start();
        }
        boolean isFirstMsg = true;
        for (SqlMsg sql : msgs) {
            txns.get(sql.getTxnId()).add(sql.getSql());
            Thread.sleep(isFirstMsg ? 150 : 8);
            isFirstMsg = false;
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

    @Test
    public void testSelectOnePrimaryKey() throws Exception {
        List<String> outputs = DBClient.execute("select name from t where id = 1",
                getIsolationLevel().getIIsolationLevel());
        Assert.assertEquals(1, outputs.size());
        Assert.assertEquals("1:name='aaa'",outputs.get(0));
    }

    @Test
    public void testSelectBySecondaryIndex() throws Exception {
        List<String> outputs = DBClient.execute("select num from t where name = 'aaa'",
                getIsolationLevel().getIIsolationLevel());
        Assert.assertEquals(1, outputs.size());
        Assert.assertEquals("1:num=100",outputs.get(0));


    }

    @Test
    public void testSelectByNonIndex() throws Exception{
        List<String> outputs = DBClient.execute("select id from t where num = 100",
                getIsolationLevel().getIIsolationLevel());
        Assert.assertEquals(1, outputs.size());
        Assert.assertEquals("1:id=1",outputs.get(0));
    }

    @Test
    public void testSelectByMultiplePredicate() throws Exception{
        List<String> outputs = DBClient.execute("select id,name from t where name > 'aaa' and num <= 200",
                getIsolationLevel().getIIsolationLevel());
        Assert.assertEquals(2, outputs.size());
        Assert.assertEquals("1:id=2, name='bbb'",outputs.get(0));
        Assert.assertEquals("1:id=7, name='ccc'",outputs.get(1));
    }

    @Test
    public void testSelectByMultiplePredicate2() throws Exception{
        List<String> outputs = DBClient.execute("select name from t where name > 'aaa' and name <= 'bbc'",
                getIsolationLevel().getIIsolationLevel(), true);
        Assert.assertEquals(2, outputs.size());
        Assert.assertEquals("1:[SEC_IDX]name='bbb'",outputs.get(0));
        Assert.assertEquals("1:[SEC_IDX]name='bbb'",outputs.get(1));
    }

    @Test
    public void testSelectByMultiplePredicate3() throws Exception {
        List<String> outputs = DBClient.execute("select * from t where id > 10 and id < 5",
                getIsolationLevel().getIIsolationLevel(), true);
        Assert.assertEquals(1, outputs.size());
        Assert.assertEquals("1:no result found",outputs.get(0));
    }

    @Test
    public void testInsertFailed() throws Exception {
        List<String> outputs = DBClient.execute("insert t values(1,'1',1)",
                getIsolationLevel().getIIsolationLevel(), true);
        Assert.assertEquals(1, outputs.size());
        Assert.assertEquals("1:0 rows affected",outputs.get(0));
    }

    @Test
    public void testInsertSuccess() throws Exception {
        IIsolationLevel isolationLevel = getIsolationLevel().getIIsolationLevel();
        List<String> outputs = DBClient.execute("insert t values(5,'5',5)",
                isolationLevel);
        Assert.assertEquals(1, outputs.size());
        Assert.assertEquals("1:1 rows affected",outputs.get(0));
        outputs = DBClient.execute("select * from t where id = 5",
                isolationLevel, true);
        Assert.assertEquals(1, outputs.size());
        Assert.assertEquals("1:id=5, name='5', num=5",outputs.get(0));
    }

    @Test
    public void testUpdateZeroRow() throws Exception{
        List<String> outputs = DBClient.execute("update t set num=100 where num = 100",
                getIsolationLevel().getIIsolationLevel(), true);
        Assert.assertEquals(1, outputs.size());
        Assert.assertEquals("1:0 rows affected",outputs.get(0));
    }

    @Test
    public void testUpdateOneRow() throws Exception{
        IIsolationLevel isolationLevel = getIsolationLevel().getIIsolationLevel();
        List<String> outputs = DBClient.execute("update t set num=100 where id = 2",
                isolationLevel);
        Assert.assertEquals(1, outputs.size());
        Assert.assertEquals("1:1 rows affected",outputs.get(0));
        outputs = DBClient.execute("select * from t where id = 2",
                isolationLevel, true);
        Assert.assertEquals(1, outputs.size());
        Assert.assertEquals("1:id=2, name='bbb', num=100",outputs.get(0));
    }

    @Test
    public void testUpdateTwoRow() throws Exception{
        IIsolationLevel isolationLevel = getIsolationLevel().getIIsolationLevel();
        List<String> outputs = DBClient.execute("update t set num=0 where name = 'bbb'",
                isolationLevel);
        Assert.assertEquals(1, outputs.size());
        Assert.assertEquals("1:2 rows affected",outputs.get(0));
        outputs = DBClient.execute("select * from t",
                isolationLevel, true);
        Assert.assertEquals(4, outputs.size());
        Assert.assertEquals("1:id=1, name='aaa', num=100",outputs.get(0));
        Assert.assertEquals("1:id=2, name='bbb', num=0",outputs.get(1));
        Assert.assertEquals("1:id=3, name='bbb', num=0",outputs.get(2));
        Assert.assertEquals("1:id=7, name='ccc', num=200",outputs.get(3));
    }
}
