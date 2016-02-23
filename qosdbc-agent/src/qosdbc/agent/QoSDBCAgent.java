/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.agent;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.util.List;
import qosdbc.commons.DatabaseSystem;
import qosdbc.commons.OutputMessage;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class QoSDBCAgent extends Thread {

    private int localPort;
    private long time;
    private List<DatabaseSystem> databaseSystems;
    private String vmId;
    private Connection catalogConnection;
    private Connection logConnection;
    private ServerSocket serverSocket;

    public QoSDBCAgent(String vmId, int localPort, long time, Connection catalogConnection, Connection logConnection, List<DatabaseSystem> databaseSystems) {
        this.vmId = vmId;
        this.localPort = localPort;
        this.time = time;
        this.catalogConnection = catalogConnection;
        this.logConnection = logConnection;
        this.databaseSystems = databaseSystems;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(localPort);
            OutputMessage.println("Agent listener is running");
            while (serverSocket != null && !serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                QoSDBCAgentThread thread = new QoSDBCAgentThread(socket, catalogConnection, logConnection, databaseSystems, vmId);
                thread.start();
            }
        } catch (IOException ex) {
            OutputMessage.println("ERROR: " + ex.getMessage());
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
    }
}