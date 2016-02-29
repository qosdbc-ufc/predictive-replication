/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.forecast;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;

/**
 *
 * @author Leonardo Oliveira Moreira
 *
 * Class that implements the ARIMA forecast approach using R
 */
public class ForecastServiceARIMAImpl implements ForecastService {

    @Override
    public double[] execute(double[] serie, int horizon) {
        double[] result = null;
        String timeSeries = "";
        for (double d : serie) {
            timeSeries += d + ",";
        }
        timeSeries = timeSeries.substring(0, timeSeries.length() - 1);
        try {
            ProcessBuilder pb = new ProcessBuilder();
            pb.command("R", "--no-save");
            pb.redirectErrorStream(true);

            Process p = pb.start();

            OutputStream out = p.getOutputStream();

            InputStreamReader isr = new InputStreamReader(p.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            String line = null;

            out.write("library(forecast)\n".getBytes());
            out.flush();

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("This is forecast")) {
                    break;
                }
            }

            out.write("library(tseries)\n".getBytes());
            out.flush();
            out.write("library(fracdiff)\n".getBytes());
            out.flush();
            out.write("library(quadprog)\n".getBytes());
            out.flush();
            out.write("library(zoo)\n".getBytes());
            out.flush();

            String autoArima = "arimafit <- auto.arima(c(" + timeSeries + "))\n";
            
            out.write(autoArima.getBytes());
            out.flush();

            String forecast = "fcast <- forecast(arimafit, h=" + horizon + ")\n";
            out.write(forecast.getBytes());
            out.flush();
            out.write("fcast$mean\n".getBytes());
            out.flush();

            line = null;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.length() > 0 && line.startsWith("[")) {
                    line = line.replaceAll("[ ]+", " ");
                    String[] temp = line.split(" ");
                    result = new double[temp.length - 1];
                    for (int i = 0; i < result.length; i++) {
                        result[i] = Double.parseDouble(temp[i + 1]);
                    }
                    p.exitValue();
                    return result;
                }
            }
        } catch (Exception ex) {
            return result;
        }
        return result;
    }
}