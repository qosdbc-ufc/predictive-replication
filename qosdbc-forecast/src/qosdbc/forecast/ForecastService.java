/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package qosdbc.forecast;

/**
 *
 * @author Leonardo Oliveira Moreira
 * 
 * Interface used to standardize calls prediction
 */
public interface ForecastService {
    
    /**
     * Method used to forecast a time series
     * 
     * @param serie
     * @param horizon
     * @return 
     */
    public double[] execute(double[] serie, int horizon);
}