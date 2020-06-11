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
        if (isTxnProtect() && myPrimaryId == 7) {
            // to avoid 2 thread run together in serial mode which make deadlock,
            // because we have not implement deadlock detection yet
            Thread.sleep(150);
        }
        List<String> res = DBClient.execute("select * from t where num = 200", isolationLevel);
        if (!isTxnProtect()) {
            Thread.sleep(50);
        }
        if (res.size() >= 2) {
            DBClient.execute("update t set num = -1 where id = " + myPrimaryId, isolationLevel);
        }
        // wait second thread wake and do, to make write skew happen
        if (isTxnProtect() && myPrimaryId != 7) {
            Thread.sleep(170);
        }
        DBClient.execute("commit", isolationLevel);
        return null;
    }

    private boolean isTxnProtect() {
        return isolationLevel != IsolationLevel.NO;
    }
}
