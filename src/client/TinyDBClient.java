package client;

import dbms.TinyDBEngine;

import java.util.HashMap;
import java.util.Map;

public class TinyDBClient {
    private final TinyDBEngine engine = new TinyDBEngine();
    private final Map<Integer,Integer> tid2prevLsn = new HashMap<>();
    private Map<Integer,Integer> snapshot= new HashMap<>();
    public synchronized String doit(String op) {
        if ("flush".equals(op)) {
            engine.flush();
        } else if ("flush-log".equals(op)) {
            // this function is used for lab3
            engine.flushLog();
        } else if (op.startsWith("t")) {
            int idx = op.indexOf("-");
            int tid = Integer.parseInt(op.substring(1, idx));
            String tOp = op.substring(idx + 1);
            if (tOp.charAt(0) == 'u') {
                int pid = Integer.parseInt(tOp.substring(3));
                tid2prevLsn.put(tid, engine.increment(pid, tid, tid2prevLsn.get(tid)));
            } else if ("cmt".equals(tOp)) {
                tid2prevLsn.put(tid, engine.commit(tid, tid2prevLsn.get(tid)));
                snapshot = new HashMap<>(tid2prevLsn);
                engine.txnEnd(tid, tid2prevLsn.get(tid));
            } else if ("abt".equals(tOp)) {
                tid2prevLsn.put(tid, engine.abort(tid, tid2prevLsn.get(tid)));
                snapshot = new HashMap<>(tid2prevLsn);
                engine.txnEnd(tid, tid2prevLsn.get(tid));
            } else {
                throw new IllegalArgumentException();
            }
        } else if ("cp-st".equals(op)) {
            engine.startCheckpoint();
        } else if ("cp-ed".equals(op)) {
            engine.endCheckpoint();
        } else if ("slct".equals(op)) {
            return engine.selectAll();
        } else if ("show-log".equals(op)) {
            return engine.showLog();
        } else {
            throw new IllegalArgumentException(op);
        }
        return "";
    }

    public synchronized void crash() {
        tid2prevLsn.clear();
        engine.crash();
    }

    public synchronized boolean start() {
        if (engine.recover(0)) {
            tid2prevLsn.putAll(snapshot);
            return true;
        }
        return false;
    }

    public synchronized boolean start(double crashPossibilty) {
        return engine.recover(crashPossibilty);
    }
}
