/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.forecast;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class ForecastTest {

    public static void main(String[] args) {
        final String monitoringFileStr = "/home/leoomoreira/Downloads/YCSB_1.csv";
        final int monitoringPointsTotal = 15;
        final int horizonTotal = 5;
        
        final String forecastingResultFileStr = monitoringFileStr.substring(0, monitoringFileStr.lastIndexOf("/") + 1) + monitoringFileStr.substring(monitoringFileStr.lastIndexOf("/") + 1, monitoringFileStr.length() - 4) + "-m" + monitoringPointsTotal + "_h" + horizonTotal + "_" + System.currentTimeMillis() + ".csv";
        
        try {
            ForecastService forecastService = new ForecastServiceARIMAImpl();
            
            File monitoringFile = new File(monitoringFileStr);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(monitoringFile));
            
            File forecastingResultFile = new File(forecastingResultFileStr);
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(forecastingResultFile));
            
            List<Double> monitoringPoints = new ArrayList<Double>();
            List<Double> forecastingPoints = new ArrayList<Double>();
            
            int lineCount = 1;
            while (bufferedReader.ready()) {
                String line = bufferedReader.readLine();
                String time = line.split(";")[0];
                String responseTime = line.split(";")[1];
                
                if (monitoringPoints.size() < monitoringPointsTotal) {
                    monitoringPoints.add(Double.parseDouble(responseTime));
                    
                    if (forecastingPoints.size() > 0) {
                        bufferedWriter.write(time + ";" + responseTime + ";" + (long)((double) forecastingPoints.remove(0)) + "\n");
                    } else {
                        bufferedWriter.write(time + ";" + responseTime + ";" + "" + "\n");
                    }
                    System.out.println("Linha: " + lineCount);
                    lineCount++;
                    
                } else {
                    monitoringPoints.add(Double.parseDouble(responseTime));
                    
                    double[] serie = new double[monitoringPoints.size()];
                    for (int i = 0; i < monitoringPoints.size(); i++) {
                        serie[i] = monitoringPoints.get(i);
                    }
                    
                    monitoringPoints = new ArrayList<Double>();
                    
                    double[] result = forecastService.execute(serie, horizonTotal);
                    for (int i = 0; i < result.length; i++) {
                        forecastingPoints.add(result[i]);
                    }
                    
                    bufferedWriter.write(time + ";" + responseTime + ";" + "" + "\n");
                    System.out.println("Linha: " + lineCount);
                    lineCount++;
                }
            }
            
            bufferedWriter.close();
            
            bufferedReader.close();
        } catch (IOException ex) {

        }
    }

}