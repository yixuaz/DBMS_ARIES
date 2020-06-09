package isolationlevel;

import dbengine.transaction.IsolationLevel;
import isolationlevel.model.SqlMsg;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ReadCommitedTest extends BaseTest {

    @Override
    public IsolationLevel getIsolationLevel() {
        return IsolationLevel.RC;
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
        testTemplate(2, input, output);
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
        testTemplate(2, input, output);
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
        testTemplate(2, input, output);
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
        testTemplate(2, input, output);
    }




}
