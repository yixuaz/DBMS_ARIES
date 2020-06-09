package isolationlevel.model;

public class SqlMsg {
    int txnId;
    String sql;

    public SqlMsg(int txnId, String sql) {
        this.txnId = txnId;
        this.sql = sql;
    }

    public int getTxnId() {
        return txnId;
    }

    public String getSql() {
        return sql;
    }
}
