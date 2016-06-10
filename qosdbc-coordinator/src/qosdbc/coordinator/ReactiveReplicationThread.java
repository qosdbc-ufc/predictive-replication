package qosdbc.coordinator;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import qosdbc.commons.DatabaseSystem;
import qosdbc.commons.OutputMessage;
import qosdbc.commons.command.Command;
import qosdbc.commons.command.CommandCode;


/**
 * Created by serafim on 01/06/2016.
 */
public class ReactiveReplicationThread extends Thread {

    private boolean runThread = true;
    private long startTime;
    private int arimaHorizon = 15;
    private Connection logConnection = null;
    private int timePeriodInSeconds = 60000;
    private int timeInterval = 30;
    private String vmId;
    private String dbname;
    private Connection catalogConnection;
    private QoSDBCService qosdbcService = null;
    private int numberOfReplicas = 0;
    private boolean pauseThread = false;
    private double sla;
    private int MAX_NUMBER_OF_VIOLATIONS_IN_A_ROW = 1;
    private int violations = 0;
    private long currentTime;
    private long windowTimestamp;
    private int timeToSleep;
    int workTime = 0;
    long query_rts_start;
    long timeOfRt;

    public ReactiveReplicationThread(Connection logConnection,
                            Connection catalogConnection,
                            QoSDBCService qosdbcService,
                            int timeIntervalInSeconds,
                            String vmId,
                            String dbname,
                            double sla) {
        this.catalogConnection = catalogConnection;
        this.logConnection = logConnection;
        this.qosdbcService = qosdbcService;
        this.vmId = vmId;
        this.dbname = dbname;
        this.sla = sla;
    }

    @Override
    public void run() {
        OutputMessage.println("[ReactiveReplicationThread("+vmId+"/"+dbname+")] running...");
        startTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        String rtOutput = "";
        while (runThread) {
            try {
                while(pauseThread) {
                    Thread.sleep(1000);
                }

                rtOutput = "";
                // seconds to sleep
                timeToSleep = timePeriodInSeconds - workTime;
                if (timeToSleep > 0) {
                    //OutputMessage.println("SlEEP " + dbname + ": " + timeToSleep);
                    Thread.sleep(timeToSleep);
                } else {
                    OutputMessage.println("WARNING: " + dbname + " " + "WorkTime larger than sleep");
                }
                while(pauseThread) {
                    Thread.sleep(1000);
                }
                query_rts_start = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                double series = qosdbcService.getResponseTime(this.dbname);
                timeOfRt = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                /*if (series == null) {
                    OutputMessage.println("ERROR - NO DATA FOR ReactiveReplicationThread");
                    break;
                }*/
                if (series >= 0) {
                    rtOutput = "[ReactiveReplicationThread("+vmId+"/"+dbname+") Last sla: " + series;
                    OutputMessage.println(rtOutput);
                    if (series > this.sla) {
                        violations++;
                    } else {
                        violations = 0;
                    }

                    if (violations == MAX_NUMBER_OF_VIOLATIONS_IN_A_ROW) {
                        violations = 0;
                        //if (numberOfReplicas < 2) {
                        //    numberOfReplicas++;
                            createReplica();
                        //}

                    }
                    Thread t = this.qosdbcService.flushTempLogBlocking(this.dbname);
                    t.start();
                    t.join();
                    logSla(series, timeOfRt);
                    workTime =  (int)(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - query_rts_start);
                    OutputMessage.println("WORK " + this.dbname + ": " + workTime);
                }
            } catch (InterruptedException ex) {
                OutputMessage.println("ERROR - Error in ReactiveReplicationThread thread");
            }
        }


    }


    private void logSla(double rt, long time) {
        String sql = "INSERT INTO sla_log VALUES ('"+ this.dbname +"', " + rt + ", " + time + ")";
        try {
            Statement statement = logConnection.createStatement();
            statement.executeUpdate(sql);
            statement.close();
        } catch (SQLException ex) {
            OutputMessage.println("ERROR: " + ex.getMessage());
        }
    }

    /**
     * It queries response times within the last 30 seconds
     *
     * @return time series
     */
    private double getSeries() {
        currentTime = startTime + 60000;
        /*int ret = this.qosdbcService.updateLog();
        if (ret == -1) {
            OutputMessage.println("ERROR -  qosdbcService could not update log!");
        }*/
        ArrayList<Double> responseTimes = new ArrayList<Double>();
        String sql = "SELECT response_time, time_local FROM sql_log WHERE db_name = '" + dbname
                + "' AND time_local >= '" + startTime + "' AND time_local < '" + currentTime + "' ORDER BY time_local ASC;";
        //String sql = "SELECT response_time FROM sql_log WHERE vm_id = '" + vmId +
        //        "' AND db_name = '" + dbname + "';";
        //OutputMessage.println(sql);

        try {
            Statement statement = logConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            startTime = currentTime;
            while (resultSet.next()) {
                double rt = resultSet.getDouble("response_time");
                responseTimes.add(rt);
                windowTimestamp = resultSet.getLong("time_local");
            }
            OutputMessage.println("[RECORDING] " + dbname + "# " + responseTimes.size());
            statement.close();
        } catch (SQLException ex) {
            OutputMessage.println("ERROR -  Could not query response times from Log: " + ex.getMessage());
        }
        return filterData(responseTimes);
    }

    private double filterData(ArrayList<Double> input) {
        double rt = 0.0;
        for (int i=0;i<input.size();i++) {
            rt += input.get(i);
        }
        rt /= input.size();
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


    private void createReplica() {
        String sourceHost = vmId;
        String databaseName = dbname;
        String databaseSystem = String.valueOf(DatabaseSystem.TYPE_MYSQL);
        String destinationHost = getHostForNewReplica();

        if (destinationHost == null) {
            // ERROR
            // No available hosts anymore
            OutputMessage.println("ERROR -  Could not create new replica of " +
                    dbname + " - No available hosts!");
            return;
        }

        Command command = new Command();
        command.setCode(CommandCode.TERMINAL_MIGRATE);
        HashMap<String, Object> commandParams = new HashMap<String, Object>();
        commandParams.put("sourceHost", sourceHost);
        commandParams.put("databaseName", databaseName);
        commandParams.put("databaseSystem", databaseSystem);
        commandParams.put("destinationHost", destinationHost);
        command.setParameters(commandParams);

        ReplicationThread replicationThread = new ReplicationThread(command,
                catalogConnection,
                logConnection,
                qosdbcService,
                qosdbcService.getLoadBalancer());
        replicationThread.start();
        numberOfReplicas++;
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

    public String getHostForNewReplica() {
        String newReplicaHost = null;

        ArrayList<String> allHosts = getAvailableHosts();
        ArrayList<String> replicaHosts = getReplicaHosts();

        for (String target : allHosts) {
            if (target.equals(this.vmId)) continue;
            if (replicaHosts.contains(target)) continue;
            newReplicaHost = target;
            break;
        }
        return newReplicaHost;
    }

    public ArrayList<String> getAvailableHosts() {
        ArrayList<String> vmLists = new ArrayList<String>();

        String sql = "SELECT vm_id FROM vm_active";
        try {
            Statement statement = catalogConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String host = resultSet.getString("vm_id");
                vmLists.add(host);
            }
        } catch (SQLException ex) {
            OutputMessage.println("ERROR -  Could not query the possible target for new replica");
        }
        return vmLists;
    }

    public ArrayList<String> getReplicaHosts() {
        ArrayList<String> vmLists = new ArrayList<String>();

        String sql = "SELECT vm_id FROM db_active_replica";
        try {
            Statement statement = catalogConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String host = resultSet.getString("vm_id");
                vmLists.add(host);
            }
        } catch (SQLException ex) {
            OutputMessage.println("ERROR -  Could not query replicas list of " +
                    dbname);
        }
        return vmLists;
    }
}
