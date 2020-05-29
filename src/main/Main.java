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
