package lock;

import dbengine.transaction.HoldLock;
import dbengine.transaction.IIsolationLevel;
import dbengine.transaction.IsolationLevel;
import dbms.DBClient;
import dbms.SystemCatalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import serverlayer.model.InvalidSqlException;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.List;

public abstract class BaseTest {
    abstract public IsolationLevel getIsolationLevel();

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
        SystemCatalog.reset();
    }

    @After
    public void restoreStreams() {
        System.setOut(originalOut);
    }

    protected void testTemplate(String sql, List<String> assertContents) throws IOException, InvalidSqlException {
        IIsolationLevel isolationLevel = getIsolationLevel().getIIsolationLevel();
        DBClient.execute(sql, isolationLevel);
        int i = 0;
        // check sql correct
        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                new ByteArrayInputStream(outContent.toByteArray())))) {
            String content;
            while ((content = br.readLine()) != null) {
                Assert.assertEquals(content, assertContents.get(i++));
            }
        }
        // check lock correct
        List<HoldLock> locks = isolationLevel.getHoldLocks();
        int j = 0;
        for (; i < assertContents.size(); i++) {
            Assert.assertEquals(assertContents.get(i), locks.get(j++).toString());
        }
        Assert.assertTrue(j == locks.size());
    }
}
