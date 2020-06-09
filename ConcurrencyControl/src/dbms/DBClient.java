package dbms;

import dbengine.DBEngine;
import dbengine.storage.ITuple;
import serverlayer.DBServer;
import serverlayer.model.InvalidSqlException;
import serverlayer.model.PhysicalPlan;
import dbengine.transaction.IIsolationLevel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DBClient {
    public static List<String> execute(String sql, IIsolationLevel isolationLevel) throws InvalidSqlException, IOException {
        sql = sql.trim().toLowerCase();
        PhysicalPlan physicalPlan = DBServer.generatePhysicalPlan(sql, isolationLevel);
        DBEngine dbEngine = new DBEngine(physicalPlan);
        List<String> ret = new ArrayList<>();
        if (sql.startsWith("select")) {
            boolean haveData = false;
            while (true) {
                String output = dbEngine.getNextRow();
                if (output == null) break;
                haveData = true;
                ret.add(physicalPlan.getTxnId() + ":" + output);
                System.out.println(ret.get(ret.size() - 1));
            }
            if (!haveData) {
                ret.add(physicalPlan.getTxnId() + ":no result found");
                System.out.println(ret.get(ret.size() - 1));
            }
        } else if (sql.equals("commit")) {
            dbEngine.commit();
        } else if (sql.startsWith("update")) {
            int updatedRow = 0;
            while (true) {
                int cnt = dbEngine.updateNextRow();
                if (cnt < 0) break;
                updatedRow += cnt;
            }
            ret.add(physicalPlan.getTxnId() + ":" + updatedRow + " rows affected");
            System.out.println(ret.get(ret.size() - 1));
        } else if (sql.startsWith("insert")) {
            ITuple success = dbEngine.insert();
            ret.add(physicalPlan.getTxnId() + ":" + (success != null ? 1 : 0) + " rows affected");
            System.out.println(ret.get(ret.size() - 1));
        }
        return ret;
    }
}
