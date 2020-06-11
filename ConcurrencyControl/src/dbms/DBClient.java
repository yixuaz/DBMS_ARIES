package dbms;

import dbengine.DBEngine;
import dbengine.storage.ITuple;
import dbengine.transaction.IsolationLevel;
import serverlayer.DBServer;
import serverlayer.model.InvalidSqlException;
import serverlayer.model.PhysicalPlan;
import dbengine.transaction.IIsolationLevel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DBClient {
    private static IIsolationLevel DEFAULT_ISOLATION_LEVEL = IsolationLevel.RR.getIIsolationLevel();
    public static List<String> execute(String sql, IIsolationLevel isolationLevel) throws InvalidSqlException {
        return execute(sql, isolationLevel, false);
    }
    public static List<String> execute(String sql, IIsolationLevel isolationLevel, boolean autoCommit) throws InvalidSqlException {
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
        if (autoCommit) {
            dbEngine.commit();
        }
        return ret;
    }

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        StringBuilder inputBuilder = new StringBuilder();
        while (true) {
            if (inputBuilder.length() == 0) {
                System.out.print("tinyDB");
            }
            System.out.print(">");
            String input = sc.nextLine();
            if ("q".equals(input) || "exit".equals(input)) {
                return;
            }
            inputBuilder.append(input);
            if (inputBuilder.charAt(inputBuilder.length() - 1) != ';') {
                inputBuilder.append(" ");
                continue;
            }
            input = inputBuilder.deleteCharAt(inputBuilder.length() - 1).toString();
            inputBuilder = new StringBuilder();
            try {
                execute(input, DEFAULT_ISOLATION_LEVEL);
            } catch (Exception e) {
                System.out.println("invalid sql, please check");
            }
        }
    }
}
