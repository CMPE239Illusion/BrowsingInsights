/**
 * Created by maverick on 5/5/17.
 */
public class HistoryRecord {
    private long T1;
    private long T2;
    private String hostname;
    private String pathname;

    public long getT1() {
        return T1;
    }

    public void setT1(long t1) {
        T1 = t1;
    }

    public long getT2() {
        return T2;
    }

    public void setT2(long t2) {
        T2 = t2;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getPathname() {
        return pathname;
    }

    public void setPathname(String pathname) {
        this.pathname = pathname;
    }
}
