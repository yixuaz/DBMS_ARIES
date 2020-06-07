package main2;

import dbengine.transaction.IIsolationLevel;
import dbengine.transaction.IsolationLevel;
import dbms.DBClient;
import serverlayer.model.InvalidSqlException;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InvalidSqlException {
        IIsolationLevel isolationLevel = IsolationLevel.RR.getIIsolationLevel();
        DBClient.doit("select * from t where num = 200 for update", isolationLevel);
        isolationLevel.printLockInfo();

        System.out.println();
        isolationLevel = IsolationLevel.RR.getIIsolationLevel();
        DBClient.doit("select * from t where num = 200", isolationLevel);
        isolationLevel.printLockInfo();

        System.out.println();
        isolationLevel = IsolationLevel.RR.getIIsolationLevel();
        DBClient.doit("select * from t where id = 2 in share mode", isolationLevel);
        isolationLevel.printLockInfo();

        System.out.println();
        isolationLevel = IsolationLevel.RR.getIIsolationLevel();
        DBClient.doit("select * from t where id < 6 for update", isolationLevel);
        isolationLevel.printLockInfo();

        System.out.println();
        isolationLevel = IsolationLevel.RR.getIIsolationLevel();
        DBClient.doit("select * from t where id >= 18 for update", isolationLevel);
        isolationLevel.printLockInfo();

        System.out.println();
        isolationLevel = IsolationLevel.RR.getIIsolationLevel();
        DBClient.doit("select * from t where id = 6 for update", isolationLevel);
        isolationLevel.printLockInfo();

        System.out.println();
        isolationLevel = IsolationLevel.RR.getIIsolationLevel();
        DBClient.doit("select * from t where name = 'bbb' for update", isolationLevel);
        isolationLevel.printLockInfo();

        System.out.println();
        isolationLevel = IsolationLevel.RR.getIIsolationLevel();
        DBClient.doit("select * from t where name > 'bbb' for update", isolationLevel);
        isolationLevel.printLockInfo();

        System.out.println();
        isolationLevel = IsolationLevel.RR.getIIsolationLevel();
        DBClient.doit("select * from t where name = 'bbc' for update", isolationLevel);
        isolationLevel.printLockInfo();

        System.out.println();
        isolationLevel = IsolationLevel.RR.getIIsolationLevel();
        DBClient.doit("select * from t where name > 'ccc' for update", isolationLevel);
        isolationLevel.printLockInfo();
    }
}
