import dbengine.transaction.IsolationLevel;
import model.SqlMsg;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        testTemplate(2, input, output);
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
        testTemplate(3, input, output);
    }


}
