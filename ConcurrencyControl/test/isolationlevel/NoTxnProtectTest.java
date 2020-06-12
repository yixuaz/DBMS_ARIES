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

public class NoTxnProtectTest extends BaseTest {

    @Override
    public IsolationLevelType getIsolationLevel() {
        return IsolationLevelType.NO;
    }

    @Test
    public void testHaveDirtyWrite() throws Exception {
        List<SqlMsg> input = new ArrayList<>();
        input.add(new SqlMsg(0, "update t set num = 10 where id = 1"));
        input.add(new SqlMsg(1, "update t set num = 11 where id = 1"));
        input.add(new SqlMsg(0, "select * from t where id = 1"));
        input.add(new SqlMsg(0, "commit"));
        input.add(new SqlMsg(1, "select * from t where id = 1"));
        input.add(new SqlMsg(1, "commit"));
        List<String> output = new ArrayList<>();
        output.add("1:1 rows affected");
        output.add("2:1 rows affected");
        output.add("1:id=1, name='aaa', num=11");
        output.add("2:id=1, name='aaa', num=11");
        concurrentTestTemplate(2, input, output);
    }

    @Test
    public void testHaveDirtyRead() throws Exception {
        List<SqlMsg> input = new ArrayList<>();
        input.add(new SqlMsg(0, "select * from t where num = 10"));
        input.add(new SqlMsg(1, "update t set num = 10 where id = 1"));
        input.add(new SqlMsg(0, "select * from t where num = 10"));
        input.add(new SqlMsg(0, "commit"));
        input.add(new SqlMsg(1, "commit"));
        List<String> output = new ArrayList<>();
        output.add("1:no result found");
        output.add("2:1 rows affected");
        output.add("1:id=1, name='aaa', num=10");
        concurrentTestTemplate(2, input, output);
    }

    @Test
    public void testHaveReadSkew() throws Exception {
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
        output.add("1:id=1, name='aaa', num=10");
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
        Assert.assertTrue(res.get(0).contains("num=200"));
    }



}
