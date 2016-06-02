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
    private int timePeriodInSeconds = 30;
    private int timeInterval = 30;
    private String vmId;
    private String dbname;
    private Connection catalogConnection;
    private QoSDBCService qosdbcService = null;
    private int numberOfReplicas = 0;
    private boolean pauseThread = false;
    private double sla;
    private int MAX_NUMBER_OF_VIOLATIONS_IN_A_ROW = 4;
    private int violations = 0;

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
                    Thread.sleep(timePeriodInSeconds * 1000);
                }
                rtOutput = "";
                // seconds to sleep
                Thread.sleep(timePeriodInSeconds * 1000);

                while(pauseThread) {
                    Thread.sleep(timePeriodInSeconds * 1000);
                }

                double[] series = getSeries();
                if (series == null) {
                    OutputMessage.println("ERROR - NO DATA FOR ReactiveReplicationThread");
                    break;
                }
                if (series.length > 0) {
                    rtOutput += "[ReactiveReplicationThread("+vmId+"/"+dbname+") Last sla: ";
                    for (int i = 0; i < series.length; i++) {
                        rtOutput += String.valueOf(series[i] + " ");
                    }
                    logSla(series[0]);
                    OutputMessage.println(rtOutput);
                    if (series[0] > this.sla) {
                        violations++;
                    } else {
                        violations = 0;
                    }

                    if (violations == MAX_NUMBER_OF_VIOLATIONS_IN_A_ROW) {
                        violations = 0;
                        createReplica();
                    }
                }
            } catch (InterruptedException ex) {
                OutputMessage.println("ERROR - Error in ReactiveReplicationThread thread");
            }
        }


    }


    private void logSla(double rt) {
        String sql = "INSERT INTO sla_log VALUES ('"+ this.dbname +"', " + rt + ")";
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
    private double[] getSeries() {
        int ret = this.qosdbcService.updateLog();
        if (ret == -1) {
            OutputMessage.println("ERROR -  qosdbcService could not update log!");
        }
        long currentTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        ArrayList<Double> responseTimes = new ArrayList<Double>();
         String sql = "SELECT response_time FROM sql_log WHERE vm_id = '" + vmId +
                 "' AND db_name = '" + dbname + "' AND time_local >= '" + startTime + "' AND time_local <= '" + currentTime + "';";
        //String sql = "SELECT response_time FROM sql_log WHERE vm_id = '" + vmId +
        //        "' AND db_name = '" + dbname + "';";
        OutputMessage.println(sql);
        try {
            Statement statement = logConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            startTime = currentTime;
            while (resultSet.next()) {
                double rt = resultSet.getDouble("response_time");
                responseTimes.add(rt);
            }
        } catch (SQLException ex) {
            OutputMessage.println("ERROR -  Could not query response times from Log");
        }
        return filterData(responseTimes);
    }

    private double[] filterData(ArrayList<Double> input) {
        int dataSize = input.size();
        double numberOfDataPoints = timePeriodInSeconds / timeInterval;
        int chunkSize = (int)dataSize/(int)numberOfDataPoints;
        chunkSize++;

        int size = dataSize/chunkSize;
        List<Double> dataPoints = new ArrayList<Double>();
        int j=0;
        int outCut = 0;
        for (int i=0;i<input.size();i = i + chunkSize) {
            if (chunkSize + i >= input.size()) {
                outCut = input.size();
            } else {
                outCut = chunkSize + i;
            }
            //System.out.println("\nRange: i=" + i + " to=" + (outCut-1));
            dataPoints.add(mean(input.subList(i, outCut)));
            j++;
        }
        double[] series = new double[dataPoints.size()];
        for (int i = 0; i < series.length; i++) {
            series[i] = dataPoints.get(i);
        }
        return series;
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
