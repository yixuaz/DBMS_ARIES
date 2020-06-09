package serverlayer;

import dbengine.transaction.IIsolationLevel;
import dbms.SystemCatalog;
import serverlayer.model.InvalidSqlException;
import serverlayer.model.LogicalPlan;
import serverlayer.model.PhysicalPlan;

public class DBServer {
    // IMMUTABLE CLASS
    private static final SQLParser sqlParser = new SQLParser();
    private static final Optimizer optimizer = new Optimizer();

    public static PhysicalPlan generatePhysicalPlan(String sql, IIsolationLevel isolationLevel) throws InvalidSqlException {
        long clientId = Thread.currentThread().getId();
        Integer txnId = SystemCatalog.getTxnId(clientId);
        LogicalPlan logicalPlan = sqlParser.parse(sql, txnId);
        logicalPlan.setTxnId(txnId);
        logicalPlan.setIsolationLevel(isolationLevel);
        if (noNeedOptimizer(sql)) {
            return new PhysicalPlan(null, logicalPlan);
        }
        logicalPlan.setReadView(isolationLevel.getTxnReadView());
        return optimizer.convertToPhysicalPlan(logicalPlan);
    }

    private static boolean noNeedOptimizer(String sql) {
        return sql.equals("commit") || sql.startsWith("insert");
    }
}
