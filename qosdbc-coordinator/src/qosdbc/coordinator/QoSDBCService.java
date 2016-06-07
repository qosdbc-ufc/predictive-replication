/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    HashMap<String, ReactiveReplicationThread> reactiveReplicThreads = null;
    private boolean REACTIVE = true;

    public QoSDBCService(int qosdbcPort, Connection catalogConnection, Connection logConnection) {
        this.qosdbcPort = qosdbcPort;
        this.catalogConnection = catalogConnection;
        this.logConnection = logConnection;
        connectionProxies = new ArrayList<QoSDBCConnectionProxy>();
        qosdbcLoadBalancer = new QoSDBCLoadBalancer();
        forecastingThreads = new HashMap<String, QoSDBCForecaster>();
        reactiveReplicThreads = new  HashMap<String, ReactiveReplicationThread>();

        OutputMessage.println("QoSDBC Service is starting");
        try {
            serverSocket = new ServerSocket(qosdbcPort);
        } catch (IOException ex) {
            OutputMessage.println(ex.getMessage());
            serverSocket = null;
        }
        try {

            Properties prop = new Properties();
            InputStream propInput = null;
            String fileProperties = System.getProperty("user.dir") + System.getProperty("file.separator") + "sla.properties";
            propInput = new FileInputStream(fileProperties);
            prop.load(propInput);

            Statement statement = catalogConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT \"time\", vm_id, db_name, dbms_type from db_active");
            while (resultSet.next()) {
                String dbName = resultSet.getString("db_name");
                int dbmsType = resultSet.getInt("dbms_type");
                String vmId = resultSet.getString("vm_id");
                QoSDBCDatabaseProxy databaseProxy = null;
                switch (dbmsType) {
                    case DatabaseSystem.TYPE_MYSQL: {
                        databaseProxy = new QoSDBCDatabaseProxy("com.mysql.jdbc.Driver", "jdbc:mysql://" + vmId + ":3306/" + dbName, dbName, "root", "ufc123", vmId, true);
                        if (IsValidDb(dbName)) {
                            if (REACTIVE) {
                                ReactiveReplicationThread reactiveReplicThread = new ReactiveReplicationThread(logConnection,
                                        catalogConnection,
                                        this,
                                        Integer.parseInt(prop.getProperty(dbName + "_pinterval")),
                                        vmId,
                                        dbName,
                                        Double.parseDouble(prop.getProperty(dbName + "_sla")));
                                //reactiveReplicThread.start();
                                reactiveReplicThreads.put(vmId + dbName, reactiveReplicThread);
                            } else {
                                QoSDBCForecaster qosdbcForecaster = new QoSDBCForecaster(logConnection,
                                        catalogConnection,
                                        this,
                                        Integer.parseInt(prop.getProperty(dbName + "_pinterval")),
                                        vmId,
                                        dbName,
                                        Double.parseDouble(prop.getProperty(dbName + "_sla")));
                                //qosdbcForecaster.start();
                                forecastingThreads.put(vmId + dbName, qosdbcForecaster);
                            }
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
            if (propInput != null) propInput.close();

        } catch (SQLException ex) {
            OutputMessage.println("ERROR: " + ex.getMessage());
            serverSocket = null;
        } catch (FileNotFoundException ex) {
            OutputMessage.println("ERROR: Could not open SLA properties file!");
            System.exit(0);
        } catch (IOException ex) {
            OutputMessage.println("ERROR: Error while loading SLA properties file!");
            System.exit(0);
        }
        OutputMessage.println("The tests and creation of the databases connection were performed successfully");
    }

    private boolean IsValidDb(String dbName) {
        //return dbName.equals("twitter");
        return  (!dbName.equals("information_schema") &&
                !dbName.equals("mysql") &&
                !dbName.equals("performance_schema"));
    }

    @Override
    public void run() {
        OutputMessage.println("QoSDBC Service is running");
        while (serverSocket != null && !serverSocket.isClosed()) {
            try {
                Socket dbConnection = serverSocket.accept();
                dbConnection.setTcpNoDelay(true);
                QoSDBCConnectionProxy connectionProxy =
                        new QoSDBCConnectionProxy(this,
                                dbConnection,
                                catalogConnection,
                                logConnection,
                                qosdbcLoadBalancer);
                connectionProxies.add(connectionProxy);
                //OutputMessage.println("[Service]: " + "NEW QoSDBCConnectionProxy: " + dbConnection.toString());
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
                if (REACTIVE) {
                    if (reactiveReplicThreads.containsKey(dao.getVmId() + dao.getDbName()))
                        reactiveReplicThreads.get(dao.getVmId() + dao.getDbName()).pauseThread();
                } else {
                    if (forecastingThreads.containsKey(dao.getVmId() + dao.getDbName()))
                        forecastingThreads.get(dao.getVmId() + dao.getDbName()).pauseForecaster();
                }
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
                if (REACTIVE) {
                    if (reactiveReplicThreads.containsKey(dao.getVmId() + dao.getDbName()))
                        reactiveReplicThreads.get(dao.getVmId() + dao.getDbName()).resumeThread();
                } else {
                    if (forecastingThreads.containsKey(dao.getVmId() + dao.getDbName()))
                        forecastingThreads.get(dao.getVmId() + dao.getDbName()).resumeForecaster();
                }
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
        String vmId = proxy.getCurrentDAO().getVmId();
        String dbName = proxy.getCurrentDAO().getDbName();
        connectionProxies.remove(proxy);
        if (connectionProxies.size() == 0) this.qosdbcLoadBalancer.removeAllReplicas();
       /* if (forecastingThreads.containsKey(vmId+dbName)) {
        QoSDBCForecaster forecasterThread = forecastingThreads.get(vmId+dbName);
        if (forecasterThread != null) forecasterThread.stopForecaster();
        forecastingThreads.remove(vmId+dbName);
        }
        */
    }

    public int updateLog() {
        int ret = 0;
        ExecutorService es = Executors.newFixedThreadPool(connectionProxies.size());
        for (int i = 0; i < connectionProxies.size(); i++) {
            es.execute(connectionProxies.get(i).updateLog());
        }
        es.shutdown();
        try {
            es.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            ret = -1;
            OutputMessage.println("[Service]: " + " ERROR: On update log threads wait!");
        }
        return ret;
    }

    public QoSDBCLoadBalancer getLoadBalancer() {
        return this.qosdbcLoadBalancer;
    }

    public synchronized void startMonitoring(String vmId, String dbName) {
        if (REACTIVE) {
            ReactiveReplicationThread thread = reactiveReplicThreads.get(vmId + dbName);
            if(!thread.isAlive()) thread.start();
        } else {
            QoSDBCForecaster thread = forecastingThreads.get(vmId + dbName);
            if(!thread.isAlive()) thread.start();
        }
    }
}