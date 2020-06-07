package serverlayer;

import dbengine.transaction.IIsolationLevel;
import dbms.SystemCatalog;
import serverlayer.model.InvalidSqlException;
import serverlayer.model.LogicalPlan;
import serverlayer.model.PhysicalPlan;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DBServer {
    // IMMUTABLE CLASS
    private static final SQLParser sqlParser = new SQLParser();
    private static final Optimizer optimizer = new Optimizer();



    public static PhysicalPlan doit(String sql, IIsolationLevel isolationLevel) throws InvalidSqlException {
        if ("commit".equals(sql)) return null;
        long clientId = Thread.currentThread().getId();
        Integer txnId = SystemCatalog.getTxnId(clientId);

        LogicalPlan logicalPlan = sqlParser.parse(sql);
        logicalPlan.setTxnId(txnId);
        logicalPlan.setIsolationLevel(isolationLevel);
        logicalPlan.setReadView(isolationLevel.getTxnReadView());
        return optimizer.convertToPhysicalPlan(logicalPlan);
    }
}
