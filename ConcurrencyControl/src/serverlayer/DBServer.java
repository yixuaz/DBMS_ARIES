package serverlayer;

import dbengine.transaction.IIsolationLevel;
import dbms.SystemCatalog;
import serverlayer.model.InvalidSqlException;
import serverlayer.model.LogicalPlan;
import serverlayer.model.PhysicalPlan;

import java.util.Collections;

public class DBServer {
    private static final SQLParser sqlParser = new SQLParser();
    private static final Optimizer optimizer = new Optimizer();

    public static PhysicalPlan generatePhysicalPlan(String sql, IIsolationLevel isolationLevel) throws InvalidSqlException {
        // 1. get txn id
        long clientId = Thread.currentThread().getId();
        Integer txnId = SystemCatalog.getTxnId(clientId);
        // 2. sql -> logical plan
        LogicalPlan logicalPlan = sqlParser.parse(sql, txnId);
        logicalPlan.setTxnId(txnId);
        logicalPlan.setIsolationLevel(isolationLevel);
        // 3. logical plan -> physical plan
        if (noNeedOptimizer(sql)) {
            return new PhysicalPlan(Collections.emptyList(), logicalPlan);
        }
        logicalPlan.setReadView(isolationLevel.getTxnReadView());
        return optimizer.convertToPhysicalPlan(logicalPlan);
    }

    private static boolean noNeedOptimizer(String sql) {
        return sql.equals("commit") || sql.startsWith("insert");
    }
}
