/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.commons.command;

import java.io.Serializable;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class Return implements Serializable {

    private long startTime;
    private long finishTime;
    private int state;
    private Object resultObject;

    public Return() {
        state = CommandCode.STATE_FAILURE;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public Object getResultObject() {
        return resultObject;
    }

    public void setResultObject(Object resultObject) {
        this.resultObject = resultObject;
    }
}