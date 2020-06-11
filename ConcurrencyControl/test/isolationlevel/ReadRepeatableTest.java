package isolationlevel;

import dbengine.transaction.IsolationLevel;
import dbms.DBClient;
import isolationlevel.model.SqlMsg;
import isolationlevel.model.WriteSkewTestThread;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.FutureTask;

public class ReadRepeatableTest extends BaseTest {

    @Override
    public IsolationLevel getIsolationLevel() {
        return IsolationLevel.RR;
    }

    @Test
    public void testNoDirtyWrite() throws Exception {
        List<SqlMsg> input = new ArrayList<>();
        input.add(new SqlMsg(0, "update t set num = 10 where id = 1"));
        input.add(new SqlMsg(1, "update t set num = 11 where id = 1"));
        input.add(new SqlMsg(0, "select * from t where id = 1"));
        input.add(new SqlMsg(0, "commit"));
        input.add(new SqlMsg(1, "select * from t where id = 1"));
        input.add(new SqlMsg(1, "commit"));
        List<String> output = new ArrayList<>();
        output.add("1:1 rows affected");
        output.add("1:id=1, name='aaa', num=10");
        output.add("2:1 rows affected");
        output.add("2:id=1, name='aaa', num=11");
        concurrentTestTemplate(2, input, output);
    }

    @Test
    public void testReaptableRead() throws Exception {
        List<SqlMsg> input = new ArrayList<>();
        input.add(new SqlMsg(0, "select * from t where num = 10"));
        input.add(new SqlMsg(1, "update t set num = 10 where id = 1"));
        input.add(new SqlMsg(0, "select * from t where num = 10"));
        input.add(new SqlMsg(1, "commit"));
        input.add(new SqlMsg(0, "select * from t where num = 10"));
        input.add(new SqlMsg(0, "commit"));
        input.add(new SqlMsg(2, "select * from t where num = 10"));
        input.add(new SqlMsg(2, "commit"));
        List<String> output = new ArrayList<>();
        output.add("1:no result found");
        output.add("2:1 rows affected");
        output.add("1:no result found");
        output.add("1:no result found");
        output.add("3:id=1, name='aaa', num=10");
        concurrentTestTemplate(3, input, output);
    }

    @Test
    public void testNoPhantomRead() throws Exception {
        List<SqlMsg> input = new ArrayList<>();
        input.add(new SqlMsg(0, "select * from t where id = 10"));
        input.add(new SqlMsg(1, "insert t values(10,'ddd',100)"));
        input.add(new SqlMsg(0, "select * from t where id = 10 lock in share mode"));
        input.add(new SqlMsg(1, "commit"));
        input.add(new SqlMsg(0, "select * from t where id = 10"));
        input.add(new SqlMsg(0, "commit"));
        List<String> output = new ArrayList<>();
        output.add("1:no result found");
        output.add("2:1 rows affected");
        output.add("1:id=10, name='ddd', num=100");
        output.add("1:no result found");
        concurrentTestTemplate(2, input, output);
    }

    @Test
    public void testHaveWriteSkew() throws Exception {
        FutureTask<Void> task1 = new FutureTask<Void>(new WriteSkewTestThread(getIsolationLevel(), 2));
        FutureTask<Void> task2 = new FutureTask<Void>(new WriteSkewTestThread(getIsolationLevel(), 7));
        new Thread(task1).start(); new Thread(task2).start();
        task1.get(); task2.get();
        List<String> res = DBClient.execute("select * from t where num = 200", getIsolationLevel().getIIsolationLevel());
        DBClient.execute("commit", getIsolationLevel().getIIsolationLevel());
        Assert.assertEquals(1, res.size());
        Assert.assertEquals("3:no result found", res.get(0));
    }

    @Test
    public void testGapLockNotBlockGapLock() throws Exception {
        List<SqlMsg> input = new ArrayList<>();
        input.add(new SqlMsg(0, "select * from t where name >= 'ccc' for update"));
        input.add(new SqlMsg(1, "select * from t where name = 'ddd' for update"));
        input.add(new SqlMsg(0, "select * from t where id = 6 for update"));
        input.add(new SqlMsg(1, "select * from t where id = 5 for update"));
        input.add(new SqlMsg(1, "commit"));
        input.add(new SqlMsg(0, "commit"));
        List<String> output = new ArrayList<>();
        // if blocks happen the out put order will be changed
        output.add("1:id=7, name='ccc', num=200");
        output.add("2:no result found");
        output.add("1:no result found");
        output.add("2:no result found");
        concurrentTestTemplate(2, input, output);
    }

    @Test
    public void testInsertLockNotBlockInsertLock() throws Exception {
        List<SqlMsg> input = new ArrayList<>();
        input.add(new SqlMsg(0, "insert t values(5,'5',5)"));
        input.add(new SqlMsg(1, "insert t values(6,'6',6)"));
        input.add(new SqlMsg(0, "select * from t where id > 4"));
        input.add(new SqlMsg(1, "commit"));
        input.add(new SqlMsg(0, "commit"));
        List<String> output = new ArrayList<>();
        // if blocks second insert output will after the select output
        output.add("1:1 rows affected");
        output.add("2:1 rows affected");
        output.add("1:id=5, name='5', num=5");
        output.add("1:id=7, name='ccc', num=200");
        concurrentTestTemplate(2, input, output);
    }

    @Test
    public void testInsertLockBlockGapLock() throws Exception {
        List<SqlMsg> input = new ArrayList<>();
        input.add(new SqlMsg(0, "insert t values(5,'5',5)"));
        input.add(new SqlMsg(1, "select * from t where id > 3 for update"));
        input.add(new SqlMsg(0, "insert t values(6,'6',6)"));
        input.add(new SqlMsg(1, "commit"));
        input.add(new SqlMsg(0, "commit"));
        List<String> output = new ArrayList<>();
        // if no blocks, select will output before second insert, and not find id=6 record
        output.add("1:1 rows affected");
        output.add("1:1 rows affected");
        output.add("2:id=5, name='5', num=5");
        output.add("2:id=6, name='6', num=6");
        output.add("2:id=7, name='ccc', num=200");
        concurrentTestTemplate(2, input, output);
    }

    @Test
    public void testGapLockBlockInsertLock() throws Exception {
        List<SqlMsg> input = new ArrayList<>();
        input.add(new SqlMsg(0, "select * from t where id > 3 for update"));
        input.add(new SqlMsg(1, "insert t values(5,'5',5)"));
        input.add(new SqlMsg(0, "select * from t where id = 5 lock in share mode"));
        input.add(new SqlMsg(1, "commit"));
        input.add(new SqlMsg(0, "commit"));
        List<String> output = new ArrayList<>();
        // if no blocks, insert will output before second select
        output.add("1:id=7, name='ccc', num=200");
        output.add("1:no result found");
        output.add("2:1 rows affected");
        concurrentTestTemplate(2, input, output);
    }


}
