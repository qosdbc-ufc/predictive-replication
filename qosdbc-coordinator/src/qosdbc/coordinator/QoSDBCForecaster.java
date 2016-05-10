/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import qosdbc.commons.DatabaseSystem;
import qosdbc.commons.OutputMessage;
import qosdbc.commons.command.Command;
import qosdbc.commons.command.CommandCode;
import qosdbc.forecast.*;

/**
 *
 * @author serafim
 */
public class QoSDBCForecaster extends Thread {

    private boolean runThread = true;
    private long startTime;
    private ForecastService arima = null;
    private int arimaHorizon = 15;
    private Connection logConnection = null;
    private int timePeriodInSeconds;
    private int timeInterval = 30;
    private String vmId;
    private String dbname;
    private Connection catalogConnection;
    private QoSDBCService qosdbcService = null;
    private int numberOfReplicas = 0;
    private boolean pauseThread = false;

    public QoSDBCForecaster(Connection logConnection, 
            Connection catalogConnection,
            QoSDBCService qosdbcService,
            int timeIntervalInSeconds, 
            String vmId, 
            String dbname) {
        this.catalogConnection = catalogConnection;
        this.logConnection = logConnection;
        this.qosdbcService = qosdbcService;
        this.timePeriodInSeconds = timeIntervalInSeconds;
        arima = new ForecastServiceARIMAImpl();
        this.vmId = vmId;
        this.dbname = dbname;
    }

    @Override
    public void run() {
        OutputMessage.println("[QoSDBCForecaster("+vmId+"/"+dbname+")]Forecasting running...");
        startTime = System.currentTimeMillis();

        while (runThread) {
            try {
                while(pauseThread) {
                    Thread.sleep(timePeriodInSeconds * 1000);
                }
                String arimaOutput = "";
                // seconds to sleep
                Thread.sleep(timePeriodInSeconds * 1000);
                while(pauseThread) {
                    Thread.sleep(timePeriodInSeconds * 1000);
                }
                
                double[] series = getSeries();
                if (series == null) {
                    OutputMessage.println("ERROR - NO DATA FOR FORECASTER");
                    break;
                }
                if (series.length > 0) {
                    arimaOutput += "Series: ";
                    for (int i = 0; i < series.length; i++) {
                        arimaOutput += String.valueOf(series[i]);
                        if (i < series.length - 1) {
                            arimaOutput += ",";
                        } else {
                            arimaOutput += "";
                        }
                    }
                    
                    double[] result = arima.execute(series, arimaHorizon);
                    if (result != null) {
                        arimaOutput += "\n\nArima result = ";
                        for (int i = 0; i < result.length; i++) {
                            arimaOutput += String.valueOf(result[i]);
                            if (i < result.length - 1) {
                                arimaOutput += ";";
                            } else {
                                arimaOutput += "";
                            }
                        }
                    }
                    OutputMessage.println(arimaOutput);
                    int largestRtIndex = max(result);
                    if (shouldCreateReplica(result[largestRtIndex])) {
                        int timeInTheFuture = timeInterval*(largestRtIndex+1);
                        OutputMessage.println("SLA violation forecasted in " + 
                                timeInTheFuture + " seconds. Value = " + result[largestRtIndex]);
                        createReplica();
                    }
                }
            } catch (InterruptedException ex) {
                OutputMessage.println("ERROR - Error in QoSDBCForecaster thread");
            }
        }
    }

    /**
     * It queries response times within the last 30 seconds
     *
     * @return time series
     */
    private double[] getSeries() {
        long currentTime = System.currentTimeMillis();
        ArrayList<Double> responseTimes = new ArrayList<>();
        String sql = "SELECT response_time FROM sql_log WHERE vm_id = '" + vmId + 
                "' AND db_name = '" + dbname + "' AND time_local >= '" + startTime + "' AND time_local <= '" + currentTime + "';";
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

    public void stopForecaster() {
        runThread = false;
    }

    public void startForecaster() {
        runThread = true;
    }

    public long getStartTime() {
        return startTime;
    }

    public int getArimaHorizon() {
        return arimaHorizon;
    }

    public void setArimaHorizon(int arimaHorizon) {
        this.arimaHorizon = arimaHorizon;
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
                                                                    qosdbcService);
        replicationThread.start();
        numberOfReplicas++;
    }
    
    private boolean shouldCreateReplica(double futureResponseTime) {
        if (numberOfReplicas > 0) return false;
        return futureResponseTime > 1.5000;
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
    
    public void pauseForecaster() {
       pauseThread = true;
    }
    
    public void resumeForecaster() {
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
