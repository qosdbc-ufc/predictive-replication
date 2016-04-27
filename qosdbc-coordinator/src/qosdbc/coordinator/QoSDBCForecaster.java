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

import qosdbc.commons.OutputMessage;
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

    public QoSDBCForecaster(Connection logConnection, int timeIntervalInSeconds, String vmId, String dbname) {
        this.logConnection = logConnection;
        this.timePeriodInSeconds = timeIntervalInSeconds;
        arima = new ForecastServiceARIMAImpl();
        this.vmId = vmId;
        this.dbname = dbname;
    }

    @Override
    public void run() {
        OutputMessage.println("Forecasting running...");
        startTime = System.currentTimeMillis();

        while (runThread) {
            try {
                String arimaOutput = "";
                // seconds to sleep
                Thread.sleep(timePeriodInSeconds * 1000);
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
        double[] series = new double[responseTimes.size()];
        for (int i = 0; i < series.length; i++) {
            series[i] = responseTimes.get(i);
        }
        return filterData(series);
    }
    
    private double[] filterData(double[] data) {
        int dataSize = data.length;
        OutputMessage.println("Number of rts: " + dataSize);
        if (dataSize > 0) {
            double numberOfDataPoints = timePeriodInSeconds / timeInterval;
            double dataSizeInterval = dataSize/numberOfDataPoints;
            int size = (int)dataSize/(int)dataSizeInterval;
            double[] dataPoints = new double[size];
            int j=0;
            for (int i=0; i<dataSize; i+=dataSizeInterval) {
                if (j==size) break;
                dataPoints[j] = data[i];
                j++;
            }
        
        return dataPoints;
        }
        return null;
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
}
