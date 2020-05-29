package memory.model;

public class ActiveTxn {
    public int txId;
    public TxnStatus status;
    public int lastLsn;

    public ActiveTxn(int txId, TxnStatus status, int lastLsn) {
        this.txId = txId;
        this.status = status;
        this.lastLsn = lastLsn;
    }

    public ActiveTxn(ActiveTxn clone) {
        this(clone.txId, clone.status, clone.lastLsn);
    }

    @Override
    public String toString() {
        return "{" +
                "tId=" + txId +
                ", sta=" + status +
                ", lastLsn=" + lastLsn +
                '}';
    }
}
