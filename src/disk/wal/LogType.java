package disk.wal;

public enum LogType {
    UPDATE, ABORT, COMMIT, TX_END, CLR, CP_START, CP_END;
}
