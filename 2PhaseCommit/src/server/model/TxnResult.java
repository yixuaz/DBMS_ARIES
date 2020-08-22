package server.model;

public enum TxnResult {
    COMMIT(false), ABORT(false), UNKNOWN(false), COMPLETED_CMT(true), COMPLETED_ABT(true);

    private boolean terminalStatus;

    TxnResult(boolean terminalStatus) {
        this.terminalStatus = terminalStatus;
    }

    public boolean isTerminalStatus() {
        return terminalStatus;
    }
}
