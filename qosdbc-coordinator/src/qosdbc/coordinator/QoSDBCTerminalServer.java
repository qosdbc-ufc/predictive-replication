/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class QoSDBCTerminalServer extends Thread {

    private ServerSocket serverSocket;
    private int terminalPort;
    private Connection catalogConnection;
    private Connection logConnection;
    private QoSDBCService qosdbcService;

    public QoSDBCTerminalServer(int terminalPort, Connection catalogConnection, Connection logConnection, QoSDBCService qosdbcService) {
        this.terminalPort = terminalPort;
        this.catalogConnection = catalogConnection;
        this.logConnection = logConnection;
        this.qosdbcService = qosdbcService;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(terminalPort);
            while (serverSocket != null && !serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                QoSDBCTerminalServerThread thread = new QoSDBCTerminalServerThread(socket, catalogConnection, logConnection, qosdbcService);
                thread.start();
            }
        } catch (IOException ex) {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException ex1) {
                }
            }
            qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
            System.exit(0);
        }
    }
}
