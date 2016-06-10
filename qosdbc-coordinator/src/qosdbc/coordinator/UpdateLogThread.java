package qosdbc.coordinator;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import qosdbc.commons.OutputMessage;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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

        OutputMessage.println("[UpdateLogThread] Rows: " + tempLog.size());




/*
        String sql = "INSERT INTO sql_log (\"time\", vm_id, db_name, time_local, sql, sql_type, response_time, sla_response_time, sla_violated, connection_id, transaction_id, affected_rows, in_migration) VALUES ";

        for (int i = 0; i < tempLog.size(); i++) {
            sql += tempLog.get(i);
            if (i != tempLog.size() - 1) sql += ", ";
        }
*/
        //OutputMessage.println("LOG: " + sql);
        try {
            String fname = "temp" + (TimeUnit.NANOSECONDS.toMillis(System.nanoTime())
                    + ThreadLocalRandom.current().nextInt(1, 1000000 + 1)) + ".csv";
            FileWriter writer = new FileWriter(fname);
            for (int i=0; i<tempLog.size(); i++) {
                writer.append(tempLog.get(i));
                writer.append("\n");
            }
            writer.flush();
            writer.close();
            CopyManager copyManager = new CopyManager((BaseConnection) logConnection);
            BufferedReader reader = new BufferedReader(new FileReader(fname));
            copyManager.copyIn("COPY sql_log (\"time\", vm_id, db_name, time_local, sql, sql_type, response_time, sla_response_time, sla_violated, connection_id, transaction_id, affected_rows, in_migration)" +
                    " FROM STDIN With csv delimiter '|' escape '\\'", reader);
            OutputMessage.println("[UpdateLogThread] Finished");
            reader.close();
            File file = new File(fname);
            file.delete();
            //Statement statement = logConnection.createStatement();
            //statement.executeUpdate(sql);
            //statement.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
