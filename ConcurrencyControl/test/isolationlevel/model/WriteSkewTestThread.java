package isolationlevel.model;

import dbengine.transaction.IsolationLevel;
import dbms.DBClient;
import dbms.TransactionThread;

import java.util.List;

public class WriteSkewTestThread extends TransactionThread {
    private final int myPrimaryId;
    public WriteSkewTestThread(IsolationLevel isolationLevel, int myId) {
        super(isolationLevel, null);
        myPrimaryId = myId;
    }

    @Override
    public Void call() throws Exception {
        List<String> res = DBClient.execute("select * from t where num = 200", isolationLevel);
        if (res.size() >= 2) {
            Thread.sleep(100);
            DBClient.execute("update t set num = -1 where id = " + myPrimaryId, isolationLevel);
        }
        Thread.sleep(100);
        DBClient.execute("commit", isolationLevel);
        return null;
    }
}
