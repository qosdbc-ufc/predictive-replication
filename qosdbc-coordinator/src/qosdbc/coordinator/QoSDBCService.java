/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import qosdbc.commons.DatabaseSystem;
import qosdbc.commons.OutputMessage;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class QoSDBCService extends Thread {

    private ServerSocket serverSocket = null;
    private int qosdbcPort;
    private Connection catalogConnection;
    private Connection logConnection;
    private List<QoSDBCConnectionProxy> connectionProxies;
    private QoSDBCLoadBalancer qosdbcLoadBalancer = null;
    private QoSDBCForecaster qosdbcForecaster = null;
    HashMap<String, QoSDBCForecaster> forecastingThreads = null;

    public QoSDBCService(int qosdbcPort, Connection catalogConnection, Connection logConnection) {
        this.qosdbcPort = qosdbcPort;
        this.catalogConnection = catalogConnection;
        this.logConnection = logConnection;
        connectionProxies = new ArrayList<QoSDBCConnectionProxy>();
        qosdbcLoadBalancer = new QoSDBCLoadBalancer();
        forecastingThreads = new HashMap<String, QoSDBCForecaster>();
        
        OutputMessage.println("QoSDBC Service is starting");
        try {
            serverSocket = new ServerSocket(qosdbcPort);
        } catch (IOException ex) {
            OutputMessage.println(ex.getMessage());
            serverSocket = null;
        }
        try {
            Statement statement = catalogConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT \"time\", vm_id, db_name, dbms_type from db_active");
            while (resultSet.next()) {
                String dbName = resultSet.getString("db_name");
                int dbmsType = resultSet.getInt("dbms_type");
                String vmId = resultSet.getString("vm_id");
                QoSDBCDatabaseProxy databaseProxy = null;
                QoSDBCForecaster qosdbcForecaster = null;
                switch (dbmsType) {
                    case DatabaseSystem.TYPE_MYSQL: {
                        databaseProxy = new QoSDBCDatabaseProxy("com.mysql.jdbc.Driver", "jdbc:mysql://" + vmId + ":3306/" + dbName, dbName, "root", "ufc123", vmId, true);
                        if (!dbName.equals("information_schema") && 
                            !dbName.equals("mysql") && 
                            !dbName.equals("performance_schema")) {
                            qosdbcForecaster = new QoSDBCForecaster(logConnection, 300, vmId, dbName);
                            qosdbcForecaster.start();
                            if (qosdbcForecaster.isAlive()) {
                                OutputMessage.println("[QoSDBCService]: Forecaster("+dbName + " in " + vmId + ") is on!");
                            }
                            forecastingThreads.put(vmId+dbName, qosdbcForecaster);
                        }
                        break;
                    }
                    case DatabaseSystem.TYPE_POSTGRES: {
                       // databaseProxy = new QoSDBCDatabaseProxy("org.postgresql.Driver", "jdbc:postgresql://" + vmId + ":5432/" + dbName, dbName, "postgres", "ufc123", vmId);
                        break;
                    }
                }
                if (databaseProxy.isActive()) {
                    OutputMessage.println(dbName + " in " + vmId + " is connected");
                } else {
                    OutputMessage.println(dbName + " in " + vmId + " is not connected");
                    System.exit(0);
                }
                databaseProxy.close();
            }
            resultSet.close();
            statement.close();
        } catch (SQLException ex) {
            OutputMessage.println("ERROR: " + ex.getMessage());
            serverSocket = null;
        }
        OutputMessage.println("The tests and creation of the databases connection were performed successfully");
    }

    @Override
    public void run() {
        OutputMessage.println("QoSDBC Service is running");
        while (serverSocket != null && !serverSocket.isClosed()) {
            try {
                Socket dbConnection = serverSocket.accept();

                OutputMessage.println("Receiving a database connection request from " + dbConnection.toString());

                QoSDBCConnectionProxy connectionProxy = 
                        new QoSDBCConnectionProxy(  this, 
                                                    dbConnection, 
                                                    catalogConnection, 
                                                    logConnection, 
                                                    qosdbcLoadBalancer);
                connectionProxies.add(connectionProxy);
                OutputMessage.println("[Service]: " + "NEW QoSDBCConnectionProxy: " + dbConnection.toString());
                connectionProxy.start();
            } catch (IOException ex) {
                OutputMessage.println(ex.getMessage());
            }
        }
    }

    public synchronized void changeDatabaseConnection(String dbName) {
        for (QoSDBCConnectionProxy proxy : connectionProxies) {
            QoSDBCDatabaseProxy dao = proxy.getCurrentDAO();
            if (dao != null && dao.getDbName().equals(dbName)) {
                proxy.changeDatabaseConnection();
            }
        }
    }

    public void pauseDatabaseConnections(String dbName) {
        int numberOfConnections = 0;
        for (QoSDBCConnectionProxy proxy : connectionProxies) {
            QoSDBCDatabaseProxy dao = proxy.getCurrentDAO();
            if (dao != null && dao.getDbName().equals(dbName)) {
                proxy.pause();
                numberOfConnections++;
            }
        }
        int c = 0;
        while (c < numberOfConnections) {
            c = 0;
            for (QoSDBCConnectionProxy proxy : connectionProxies) {
                QoSDBCDatabaseProxy dao = proxy.getCurrentDAO();
                if (dao != null && dao.getDbName().equals(dbName)) {
                    if (proxy.getState() == State.WAITING) {
                        c++;
                    }
                }
            }
        }
    }

    public void playDatabaseConnections(String dbName) {
        int numberOfConnections = 0;
        for (QoSDBCConnectionProxy proxy : connectionProxies) {
            QoSDBCDatabaseProxy dao = proxy.getCurrentDAO();
            if (dao != null && dao.getDbName().equals(dbName)) {
                proxy.play();
                numberOfConnections++;
            }
        }
        int c = 0;
        while (c < numberOfConnections) {
            c = 0;
            for (QoSDBCConnectionProxy proxy : connectionProxies) {
                QoSDBCDatabaseProxy dao = proxy.getCurrentDAO();
                if (dao != null && dao.getDbName().equals(dbName)) {
                    if (proxy.getState() == State.RUNNABLE) {
                        c++;
                    }
                }
            }
        }
    }
    
    public void setInMigration(String dbName, boolean inMigration) {
        for (QoSDBCConnectionProxy proxy : connectionProxies) {
            QoSDBCDatabaseProxy dao = proxy.getCurrentDAO();
            if (dao != null && dao.getDbName().equals(dbName)) {
                proxy.setInMigration(inMigration);
            }
        }
    }

    public synchronized void removeConnectionProxy(QoSDBCConnectionProxy proxy) {
        connectionProxies.remove(proxy);
    }
}