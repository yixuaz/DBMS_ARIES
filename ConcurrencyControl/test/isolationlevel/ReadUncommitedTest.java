package isolationlevel;

import dbengine.transaction.model.IsolationLevelType;
import isolationlevel.model.SqlMsg;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ReadUncommitedTest extends BaseTest {

    @Override
    public IsolationLevelType getIsolationLevel() {
        return IsolationLevelType.RU;
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
    public void testWriteNotBlockRead() throws Exception {
        List<SqlMsg> input = new ArrayList<>();
        input.add(new SqlMsg(0, "select * from t"));
        input.add(new SqlMsg(1, "update t set num = 15 where id > 1"));
        input.add(new SqlMsg(0, "select * from t"));
        input.add(new SqlMsg(1, "update t set num = 10 where name < 'ccc'"));
        input.add(new SqlMsg(0, "select * from t"));
        input.add(new SqlMsg(0, "commit"));
        input.add(new SqlMsg(1, "commit"));
        List<String> output = new ArrayList<>();
        output.add("1:id=1, name='aaa', num=100");
        output.add("1:id=2, name='bbb', num=200");
        output.add("1:id=3, name='bbb', num=300");
        output.add("1:id=7, name='ccc', num=200");
        output.add("2:3 rows affected");
        output.add("1:id=1, name='aaa', num=100");
        output.add("1:id=2, name='bbb', num=15");
        output.add("1:id=3, name='bbb', num=15");
        output.add("1:id=7, name='ccc', num=15");
        output.add("2:3 rows affected");
        output.add("1:id=1, name='aaa', num=10");
        output.add("1:id=2, name='bbb', num=10");
        output.add("1:id=3, name='bbb', num=10");
        output.add("1:id=7, name='ccc', num=15");
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
        output.add("1:id=1, name='aaa', num=10");
        output.add("1:id=1, name='aaa', num=10");
        output.add("3:id=1, name='aaa', num=10");
        concurrentTestTemplate(3, input, output);
    }


}
