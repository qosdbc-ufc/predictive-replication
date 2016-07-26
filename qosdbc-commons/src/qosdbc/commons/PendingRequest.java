package qosdbc.commons;

public class PendingRequest {


    private long transactionId;
    private String command;

    public PendingRequest(long transactionId, String command) {
        this.transactionId = transactionId;
        this.command = command;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }
}