package main;

import client.TinyDBClient;

public class Main {
    // here is an example listed in this blog
    // https://www.jianshu.com/p/ea61881309df
    // use it to debug your code
    public static void main(String[] args) {
        TinyDBClient db = new TinyDBClient();
        String opsSequence = "t1-u-p1,t1-u-p2,cp-st,cp-ed,t1-u-p3,flush,t2-u-p4,t1-cmt,t3-u-p2,t2-u-p1,t2-cmt,t3-u-p5";
        for (String op : opsSequence.split(",")) {
            String output = db.doit(op);
            if (output.isEmpty()) {
                continue;
            }
            System.out.println(output);
        }
        System.out.println(db.doit("show-log"));
        System.out.println(db.doit("slct"));
        db.crash();
        System.out.println(db.doit("slct"));
        db.start();
        System.out.println(db.doit("slct"));
        System.out.println(db.doit("show-log"));
    }
}
/**
 * the correct output should like below:
 *
 * LogRecord{lsn=1, prevLsn=null, txId=1, type=UPDATE, pageId=1, beforeVal=0, afterVal=1, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=2, prevLsn=1, txId=1, type=UPDATE, pageId=2, beforeVal=0, afterVal=1, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=3, prevLsn=null, txId=0, type=CP_START, pageId=0, beforeVal=0, afterVal=0, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=4, prevLsn=null, txId=0, type=CP_END, pageId=0, beforeVal=0, afterVal=0, undoNextLsn=null, att={1={tId=1, sta=Running, lastLsn=2}}, dpt={1=1, 2=2}}
 * LogRecord{lsn=5, prevLsn=2, txId=1, type=UPDATE, pageId=3, beforeVal=0, afterVal=1, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=6, prevLsn=null, txId=2, type=UPDATE, pageId=4, beforeVal=0, afterVal=1, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=7, prevLsn=5, txId=1, type=COMMIT, pageId=0, beforeVal=0, afterVal=0, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=8, prevLsn=7, txId=1, type=TX_END, pageId=0, beforeVal=0, afterVal=0, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=9, prevLsn=null, txId=3, type=UPDATE, pageId=2, beforeVal=1, afterVal=2, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=10, prevLsn=6, txId=2, type=UPDATE, pageId=1, beforeVal=1, afterVal=2, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=11, prevLsn=10, txId=2, type=COMMIT, pageId=0, beforeVal=0, afterVal=0, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=12, prevLsn=11, txId=2, type=TX_END, pageId=0, beforeVal=0, afterVal=0, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=13, prevLsn=9, txId=3, type=UPDATE, pageId=5, beforeVal=0, afterVal=1, undoNextLsn=null, att=null, dpt=null}
 *
 * 0, 2, 2, 1, 1, 1, 0, 0, 0, 0
 * 0, 1, 1, 1, 0, 0, 0, 0, 0, 0
 * 0, 2, 1, 1, 1, 0, 0, 0, 0, 0
 * LogRecord{lsn=1, prevLsn=null, txId=1, type=UPDATE, pageId=1, beforeVal=0, afterVal=1, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=2, prevLsn=1, txId=1, type=UPDATE, pageId=2, beforeVal=0, afterVal=1, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=3, prevLsn=null, txId=0, type=CP_START, pageId=0, beforeVal=0, afterVal=0, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=4, prevLsn=null, txId=0, type=CP_END, pageId=0, beforeVal=0, afterVal=0, undoNextLsn=null, att={1={tId=1, sta=Running, lastLsn=2}}, dpt={1=1, 2=2}}
 * LogRecord{lsn=5, prevLsn=2, txId=1, type=UPDATE, pageId=3, beforeVal=0, afterVal=1, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=6, prevLsn=null, txId=2, type=UPDATE, pageId=4, beforeVal=0, afterVal=1, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=7, prevLsn=5, txId=1, type=COMMIT, pageId=0, beforeVal=0, afterVal=0, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=8, prevLsn=7, txId=1, type=TX_END, pageId=0, beforeVal=0, afterVal=0, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=9, prevLsn=null, txId=3, type=UPDATE, pageId=2, beforeVal=1, afterVal=2, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=10, prevLsn=6, txId=2, type=UPDATE, pageId=1, beforeVal=1, afterVal=2, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=11, prevLsn=10, txId=2, type=COMMIT, pageId=0, beforeVal=0, afterVal=0, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=12, prevLsn=11, txId=2, type=TX_END, pageId=0, beforeVal=0, afterVal=0, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=13, prevLsn=9, txId=3, type=UPDATE, pageId=5, beforeVal=0, afterVal=1, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=14, prevLsn=13, txId=3, type=CLR, pageId=5, beforeVal=1, afterVal=0, undoNextLsn=9, att=null, dpt=null}
 * LogRecord{lsn=15, prevLsn=14, txId=3, type=CLR, pageId=2, beforeVal=2, afterVal=1, undoNextLsn=null, att=null, dpt=null}
 * LogRecord{lsn=16, prevLsn=15, txId=3, type=TX_END, pageId=0, beforeVal=0, afterVal=0, undoNextLsn=null, att=null, dpt=null}
 */
