package isolationlevel;

import dbengine.transaction.model.IsolationLevelType;
import dbms.DBClient;
import isolationlevel.model.SqlMsg;
import isolationlevel.model.WriteSkewTestThread;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.FutureTask;

public class ReadCommitedTest extends BaseTest {

    @Override
    public IsolationLevelType getIsolationLevel() {
        return IsolationLevelType.RC;
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
    public void testNoDirtyReadButNonReaptableRead() throws Exception {
        List<SqlMsg> input = new ArrayList<>();
        input.add(new SqlMsg(0, "select * from t where num = 10"));
        input.add(new SqlMsg(1, "update t set num = 10 where id = 1"));
        input.add(new SqlMsg(0, "select * from t where num = 10"));
        input.add(new SqlMsg(1, "commit"));
        input.add(new SqlMsg(0, "select * from t where num = 10"));
        input.add(new SqlMsg(0, "commit"));
        List<String> output = new ArrayList<>();
        output.add("1:no result found");
        output.add("2:1 rows affected");
        output.add("1:no result found");
        output.add("1:id=1, name='aaa', num=10");
        concurrentTestTemplate(2, input, output);
    }

    @Test
    public void testNoDirtyRead2() throws Exception {
        List<SqlMsg> input = new ArrayList<>();
        input.add(new SqlMsg(0, "select * from t where id = 1"));
        input.add(new SqlMsg(1, "update t set num = 5 where id <= 4"));
        input.add(new SqlMsg(0, "select * from t where id = 2"));
        input.add(new SqlMsg(1, "commit"));
        input.add(new SqlMsg(0, "select * from t where id = 3"));
        input.add(new SqlMsg(0, "commit"));
        List<String> output = new ArrayList<>();
        output.add("1:id=1, name='aaa', num=100");
        output.add("2:3 rows affected");
        output.add("1:id=2, name='bbb', num=200");
        output.add("1:id=3, name='bbb', num=5");
        concurrentTestTemplate(2, input, output);
    }

    @Test
    public void testNoDirtyRead3() throws Exception {
        List<SqlMsg> input = new ArrayList<>();
        input.add(new SqlMsg(0, "select * from t where id = 1 "));
        input.add(new SqlMsg(1, "update t set num = 5 where id <= 1"));
        input.add(new SqlMsg(0, "select * from t where id = 1 lock in share mode"));
        input.add(new SqlMsg(0, "commit"));
        input.add(new SqlMsg(1, "select * from t where id = 1"));
        input.add(new SqlMsg(1, "commit"));
        List<String> output = new ArrayList<>();
        output.add("1:id=1, name='aaa', num=100");
        output.add("2:1 rows affected");
        output.add("2:id=1, name='aaa', num=5");
        output.add("1:id=1, name='aaa', num=5");
        concurrentTestTemplate(2, input, output);
    }

    @Test
    public void testNoReaptableRead() throws Exception {
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
        output.add("1:id=1, name='aaa', num=10");
        output.add("3:id=1, name='aaa', num=10");
        concurrentTestTemplate(3, input, output);
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


}
