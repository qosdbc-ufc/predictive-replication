/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.commons.jdbc;

import java.io.Serializable;
import java.util.HashMap;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class Request implements Serializable {

    private int code;
    private HashMap<String, Object> parameters;
    private String command;
    private String vmName;
    private String database;
    private long slaResponseTime;
    private long connectionId;
    private long transactionId;

    public Request() {
        parameters = new HashMap<String, Object>();
    }

    public long getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(long connectionId) {
        this.connectionId = connectionId;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public long getSlaResponseTime() {
        return slaResponseTime;
    }

    public void setSlaResponseTime(long slaResponseTime) {
        this.slaResponseTime = slaResponseTime;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
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