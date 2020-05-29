package disk.dbpage;

public class Page {
    private final int pageId;

    private int data;
    private Integer pageLSN;
    private Integer recLSN;

    public Page(int pageId) {
        this.pageId = pageId;
    }

    public Page(Page ori) {
        this(ori.pageId);
        copyPageFrom(ori);
    }

    public void copyPageFrom(Page newPage) {
        this.data = newPage.data;
        if (newPage.recLSN != null) {
            this.recLSN = new Integer(newPage.recLSN);
        }
        if (newPage.pageLSN != null) {
            this.pageLSN = new Integer(newPage.pageLSN);
        }

        this.data = newPage.data;
    }

    public int getPageId() {
        return pageId;
    }

    public Integer getPageLSN() {
        return pageLSN;
    }

    public Integer getRecLSN() {
        return recLSN;
    }

    public int getData() {
        return data;
    }

    public void update(int val, Integer lsn) {
        data = val;
        if (recLSN == null) {
            recLSN = lsn;
        }
        pageLSN = lsn;
    }

    public void setRecLsn(int lsn) {
        recLSN = lsn;
    }
}
