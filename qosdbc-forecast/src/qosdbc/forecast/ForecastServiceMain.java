/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.forecast;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class ForecastServiceMain {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("> Parameter error");
            System.out.println("# qosdbc-forecast > Sintax: java -jar qosdbc-forecast.jar arima|svr [x,y,z,w,k] horizon");
            System.exit(-1);
        }
        
        if (!args[0].equalsIgnoreCase("arima") && !args[0].equalsIgnoreCase("svr")) {
            System.out.println("> Parameter error");
            System.out.println("# qosdbc-forecast > Sintax: java -jar qosdbc-forecast.jar arima|svr [x,y,z,w,k] horizon");
            System.exit(-1);
        }
        
        if (args[0].equalsIgnoreCase("arima")) {
            ForecastService forecast = new ForecastServiceARIMAImpl();
            int horizon = Integer.parseInt(args[2]);
            String strSerie = args[1];
            strSerie = strSerie.substring(1, strSerie.length() - 1);
            String[] s = strSerie.split(",");
            double[] series = new double[s.length];
            for (int i = 0; i < series.length; i++) {
                series[i] = Double.parseDouble(s[i]);
            }
            double[] result = forecast.execute(series, horizon);
            if (result == null) {
                System.exit(-1);
            }
            for (int i = 0; i < result.length; i++) {
                System.out.print(result[i]);
                if (i < result.length - 1) {
                    System.out.print(";");
                } else {
                    System.out.println("");
                }
            }
            System.exit(0);
        }
    }
}