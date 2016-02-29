/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.commons.command;

import java.io.Serializable;
import java.util.HashMap;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class Command implements Serializable {

    private int code;
    private HashMap<String, Object> parameters;
    private String vmName;
    private String database;

    public Command() {
        parameters = new HashMap<String, Object>();
    }
    
    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getVmName() {
        return vmName;
    }

    public void setVmName(String vmName) {
        this.vmName = vmName;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public HashMap<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(HashMap<String, Object> parameters) {
        this.parameters = parameters;
    }

    public void addParameter(String key, Object value) {
        parameters.put(key, value);
    }
    
    public Object getParameterValue(String key) {
        return parameters.get(key);
    }
}