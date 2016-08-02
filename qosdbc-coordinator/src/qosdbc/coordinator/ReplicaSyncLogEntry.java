package qosdbc.coordinator;

public class ReplicaSyncLogEntry {

    private long time;
    private String dbName;
    private long syncTime;

    public ReplicaSyncLogEntry(long time, String dbName, long syncTime) {
        this.time = time;
        this.dbName = dbName;
        this.syncTime = syncTime;
    }

    public long getTime() {
        return time;
    }

    public String getDbName() {
        return dbName;
    }

    public long getSyncTime() {
        return syncTime;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public void setSyncTime(long syncTime) {
        this.syncTime = syncTime;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}