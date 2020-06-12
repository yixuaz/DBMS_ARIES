package dbms;

import dbengine.storage.tables.ITable;
import dbengine.storage.tables.MyTable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SystemCatalog {
    public static final int END_DUMMY_TXN_ID_TAG = Integer.MIN_VALUE;
    public static final int NULL_PRIMARY_ID = Integer.MIN_VALUE;
    private static Map<String, ITable> name2table = new HashMap<>();
    private static Map<ITable, TableSystemCatalog> name2tableConfig = new HashMap<>();
    private static final AtomicInteger txnIdGenerator = new AtomicInteger(0);
    private static final Map<Long, Integer> clientId2TxnId = new ConcurrentHashMap<>();
    static {
        reset();
    }

    public static ITable getTable(String tablename) {
        return name2table.get(tablename);
    }

    public static TableSystemCatalog getTableConfig(ITable table) {
        return name2tableConfig.get(table);
    }

    public static void reset() {
        ITable t = new MyTable();
        name2table.put("t", t);
        name2tableConfig.put(t, new TableSystemCatalog());
        txnIdGenerator.set(0);
        clientId2TxnId.clear();
    }

    public static int getTxnId(long clientId) {
        int txnId = clientId2TxnId.computeIfAbsent(clientId, k -> txnIdGenerator.incrementAndGet());
        DBEngineGlobalEnvironment.addTxnId(txnId);
        return txnId;
    }

    public static int getMaxTxnId() {
        return txnIdGenerator.get();
    }
}
