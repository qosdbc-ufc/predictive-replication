package qosdbc.coordinator;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import qosdbc.commons.OutputMessage;


/**
 * Created by serafim on 01/06/2016.
 */
public class QoSDBCLogger extends Thread {

    private boolean runThread = true;
    private long startTime;
    private Connection logConnection = null;
    private int timePeriodInSeconds = 60000;
    private String vmId;
    private String dbname;
    private Connection catalogConnection;
    private QoSDBCService qosdbcService = null;
    private boolean pauseThread = false;
    private long currentTime;
    private long windowTimestamp;
    private int timeToSleep;
    private int workTime = 0;
    private long query_rts_start;

    public QoSDBCLogger(Connection logConnection,
                                     Connection catalogConnection,
                                     QoSDBCService qosdbcService,
                                     String vmId,
                                     String dbname) {
        this.catalogConnection = catalogConnection;
        this.logConnection = logConnection;
        this.qosdbcService = qosdbcService;
        this.vmId = vmId;
        this.dbname = dbname;

    }

    @Override
    public void run() {
        startTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        OutputMessage.println("[ReactiveReplicationThread("+vmId+"/"+dbname+")] running...");
        String rtOutput = "";
        while (runThread) {
            try {
                while(pauseThread) {
                    Thread.sleep(1000);
                }
                rtOutput = "";
                // seconds to sleep
                timeToSleep = timePeriodInSeconds - workTime;
                Thread.sleep(timeToSleep);

                while(pauseThread) {
                    Thread.sleep(1000);
                }

                query_rts_start = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                double series = this.qosdbcService.getResponseTime(this.dbname);
                long rtTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                /*if (series == null) {
                    OutputMessage.println("ERROR - NO DATA FOR ReactiveReplicationThread");
                    break;
                }*/
                if (series >= 0) {
                    rtOutput = " [LoggerThread("+vmId+"/"+dbname+") Last sla: " + series;
                    logSla(series, rtTime);
                    OutputMessage.println(rtOutput);
                }
                this.qosdbcService.flushTempLog(this.dbname);
                workTime =  (int)(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - query_rts_start);
                OutputMessage.println("WORK " + this.dbname + ": " + workTime);
            } catch (InterruptedException ex) {
                OutputMessage.println("ERROR - Error in ReactiveReplicationThread thread");
            }
        }
    }


    private void logSla(double rt, long time) {
        //long query_rts_start = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        String sql = "INSERT INTO sla_log VALUES ('"+ this.dbname +"', " + rt + ", " + time + ")";
        try {
            Statement statement = logConnection.createStatement();
            statement.executeUpdate(sql);
            statement.close();
        } catch (SQLException ex) {
            OutputMessage.println("ERROR: " + ex.getMessage());
        }
        //OutputMessage.println("Total time to logSla: " + (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - query_rts_start)));
    }

    /**
     * It queries response times within the last 30 seconds
     *
     * @return time series
     */
    private double getSeries() {
        currentTime = startTime + 30000;
        /*int ret = this.qosdbcService.updateLog();
        if (ret == -1) {
            OutputMessage.println("ERROR -  qosdbcService could not update log!");
        }*/
        ArrayList<Double> responseTimes = new ArrayList<Double>();
        String sql = "SELECT response_time, time_local FROM sql_log WHERE vm_id = '" + vmId +
                "' AND db_name = '" + dbname + "' AND time_local >= '" + startTime + "' AND time_local < '" + currentTime + "' ORDER BY time_local ASC;";
        //String sql = "SELECT response_time FROM sql_log WHERE vm_id = '" + vmId +
        //        "' AND db_name = '" + dbname + "';";
        OutputMessage.println(sql);
        long query_rts_start = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        try {
            Statement statement = logConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            startTime = currentTime;
            while (resultSet.next()) {
                double rt = resultSet.getDouble("response_time");
                responseTimes.add(rt);
                windowTimestamp = resultSet.getLong("time_local");
            }
            OutputMessage.println("Total time to query rts: " + (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - query_rts_start)));
            OutputMessage.println("[RECORDING] " + dbname + "# " + responseTimes.size());
        } catch (SQLException ex) {
            OutputMessage.println("ERROR -  Could not query response times from Log: " + ex.getMessage());
        }
        return filterData(responseTimes);
    }

    private double filterData(ArrayList<Double> input) {
        long query_rts_start = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        double rt = 0.0;
        for (int i=0;i<input.size();i++) {
            rt += input.get(i);
        }
        rt /= input.size();
        OutputMessage.println("Total time to filter data: " + (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - query_rts_start)));
        return rt;
    }

    private static double mean(List<Double> data) {
        double sum = 0;
        for (int i=0;i<data.size();i++) {
            //System.out.print(data.get(i) + " ");
            sum += data.get(i);
        }
        sum /= data.size();
        return sum;
    }

    public void stopThread() {
        runThread = false;
    }

    public void startThread() {
        runThread = true;
    }

    public long getStartTime() {
        return startTime;
    }


    private int max(double[] futureResponseTimes) {
        int largestRtIndex = 0;
        double largestRt = 0;
        for (int i=0;i<futureResponseTimes.length; i++) {
            if (futureResponseTimes[i] > largestRt) {
                largestRtIndex = i;
                largestRt = futureResponseTimes[i];
            }
        }
        return largestRtIndex;
    }

    public void pauseThread() {
        pauseThread = true;
    }

    public void resumeThread() {
        pauseThread = false;
    }
}
