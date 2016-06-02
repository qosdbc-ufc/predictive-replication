package qosdbc.coordinator;

import qosdbc.commons.OutputMessage;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Created by serafim on 30/05/2016.
 */
public class UpdateLogThread extends Thread {
    private Connection logConnection = null;
    private List<String> tempLog;

    public UpdateLogThread(List<String> tempLog, Connection logConnection) {
        this.logConnection = logConnection;
        this.tempLog = tempLog;
    }

    public void run() {
        if (tempLog.isEmpty()) return;

        String sql = "INSERT INTO sql_log (\"time\", vm_id, db_name, time_local, sql, sql_type, response_time, sla_response_time, sla_violated, connection_id, transaction_id, affected_rows, in_migration) VALUES ";

        for (int i = 0; i < tempLog.size(); i++) {
            sql += tempLog.get(i);
            if (i != tempLog.size() - 1) sql += ", ";
        }

        //OutputMessage.println("LOG: " + sql);
        try {
            Statement statement = logConnection.createStatement();
            statement.executeUpdate(sql);
            statement.close();
        } catch (SQLException ex) {
            OutputMessage.println("ERROR: " + ex.getMessage());
        }
    }
}
