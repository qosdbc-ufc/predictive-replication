package qosdbc.coordinator;

import qosdbc.commons.OutputMessage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Created by serafim on 30/05/2016.
 */
public class UpdateReplicaSyncLogThread implements Runnable {
    private Connection logConnection = null;
    private List<ReplicaSyncLogEntry> tempLog;
    private String dbName;

    public UpdateReplicaSyncLogThread(List<ReplicaSyncLogEntry> tempLog, Connection logConnection, String dbName) {
        this.logConnection = logConnection;
        this.tempLog = tempLog;
        this.dbName = dbName;
    }

    public void run() {
        if (tempLog.isEmpty()) return;
        OutputMessage.println("[UpdateReplicaSyncLogThread] Rows: " + tempLog.size());
        Statement statement = null;
        try {
            statement = logConnection.createStatement();
            for (ReplicaSyncLogEntry entry : tempLog) {
                String sql = "INSERT INTO replica_sync VALUES(" + entry.getTime()
                        + ", '" + entry.getDbName() + "'"
                        + ", " + entry.getSyncTime() + ")";
                statement.addBatch(sql);
            }
            int[] count = statement.executeBatch();
        } catch (SQLException e) {
            OutputMessage.println("[UpdateReplicaSyncLogThread] SQLException: " + e.getMessage());
        }
    }
}