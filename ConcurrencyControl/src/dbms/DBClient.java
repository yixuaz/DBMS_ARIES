package dbms;

import dbengine.DBEngine;
import serverlayer.DBServer;
import serverlayer.model.InvalidSqlException;
import serverlayer.model.PhysicalPlan;
import dbengine.transaction.IIsolationLevel;

import java.io.IOException;

public class DBClient {
    public static void doit(String sql, IIsolationLevel isolationLevel) throws InvalidSqlException, IOException {
        sql = sql.trim().toLowerCase();
        PhysicalPlan physicalPlan = DBServer.doit(sql, isolationLevel);
        DBEngine dbEngine = new DBEngine(physicalPlan);
        if (sql.startsWith("select")) {
            boolean haveData = false;
            while (true) {
                String output = dbEngine.getNextRow();
                if (output == null) break;
                haveData = true;
                System.out.println(physicalPlan.getTxnId() + ":" + output);
            }
            if (!haveData) {
                System.out.println(physicalPlan.getTxnId() + ":no result found");
            }
        } else if (sql.equals("commit")) {
            dbEngine.commit(isolationLevel, SystemCatalog.getTxnId(Thread.currentThread().getId()));
        } else if (sql.startsWith("update")) {
            int updatedRow = 0;
            while (true) {
                int cnt = dbEngine.updateNextRow();
                if (cnt < 0) break;
                updatedRow += cnt;
            }
            System.out.println(physicalPlan.getTxnId() + ":" + updatedRow + " rows affected");
        }
    }
}
