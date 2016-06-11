package qosdbc.coordinator;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import qosdbc.commons.OutputMessage;

import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Created by serafim on 30/05/2016.
 */
public class UpdateLogThread implements Runnable {
    private Connection logConnection = null;
    private List<String> tempLog;
    private String dbName;

    public UpdateLogThread(List<String> tempLog, Connection logConnection, String dbName) {
        this.logConnection = logConnection;
        this.tempLog = tempLog;
        this.dbName = dbName;
    }

    public void run() {
        if (tempLog.isEmpty()) return;

        OutputMessage.println("[UpdateLogThread] # Files: " + tempLog.size());
        long start = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

        try {
            String finalFile = "/home/lsbd/coordinator/temp/" + "temp" + dbName + start + ".csv";
            FileChannel c2 = new FileOutputStream(finalFile, true).getChannel();
            for (String tempFile : tempLog) {
                FileChannel c1 = new FileInputStream(tempFile).getChannel();
                c2.transferFrom(c1, c2.size(), c1.size());
                deleteFile(tempFile);
            }
            CopyManager copyManager = new CopyManager((BaseConnection) logConnection);

            File file = new File(finalFile);
            FileReader fileReader = new FileReader(file);
            copyManager.copyIn("COPY sql_log (\"time\", vm_id, db_name, time_local, sql, sql_type, response_time, sla_response_time, sla_violated, connection_id, transaction_id, affected_rows, in_migration)" +
                " FROM STDIN With csv delimiter '|' escape '\\'", fileReader);
            file.delete();

            tempLog.clear();
            OutputMessage.println("[UpdateLogThread]: " + dbName + " Finished in " +
                (TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - start));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private void deleteFile(String fileName) {
        File temp = new File(fileName);
        temp.delete();
    }
}
