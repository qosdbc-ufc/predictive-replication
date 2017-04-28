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
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.AtomicDouble;

import qosdbc.commons.DatabaseSystem;
import qosdbc.commons.OutputMessage;
import qosdbc.commons.Pair;

/**
 * @author Leonardo Oliveira Moreira
 */
public class QoSDBCService extends Thread {

  static ConsistencyService consistencyService = null;
  private ServerSocket serverSocket = null;
  private int qosdbcPort;
  private Connection catalogConnection;
  private Connection logConnection;
  private List<QoSDBCConnectionProxy> connectionProxies;
  private QoSDBCLoadBalancer qosdbcLoadBalancer = null;
  private QoSDBCForecaster qosdbcForecaster = null;
  private HashMap<String, QoSDBCForecaster> forecastingThreads = null;
  private HashMap<String, ReactiveReplicationThread> reactiveReplicThreads = null;
  private HashMap<String, QoSDBCLogger> loggerThreads = null;
  private boolean REACTIVE = true;
  private ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
  private HashMap<String, List<Thread>> loggingThreadMap = null;
  private HashMap<String, Boolean> replicationGoingOnMap = null;
  // <<dbName,VmId>, responseTime>
  private HashMap<Pair<String, String>, AtomicDouble> responseTimeMap = null;
  //private HashMap<Pair<String, String>, Lock> responseTimeLockMap = null;
  private HashMap<Pair<String, String>, AtomicInteger> responseTimeCountMap = null;

  public QoSDBCService(int qosdbcPort, Connection catalogConnection, Connection logConnection) {
    this.qosdbcPort = qosdbcPort;
    this.catalogConnection = catalogConnection;
    this.logConnection = logConnection;
    connectionProxies = new ArrayList<QoSDBCConnectionProxy>();
    forecastingThreads = new HashMap<String, QoSDBCForecaster>();
    reactiveReplicThreads = new HashMap<String, ReactiveReplicationThread>();
    loggerThreads = new HashMap<String, QoSDBCLogger>();
    consistencyService = new ConsistencyService();
    loggingThreadMap = new HashMap<String, List<Thread>>();
    replicationGoingOnMap = new HashMap<String, Boolean>();
    responseTimeMap = new HashMap<Pair<String, String>, AtomicDouble>();
    //responseTimeLockMap = new HashMap<Pair<String, String>, Lock>();
    responseTimeCountMap = new HashMap<Pair<String, String>, AtomicInteger>();

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
            consistencyService.addTenantAtHost(dbName, vmId);
            if (IsValidDb(dbName)) {

              ArrayList<Thread> loggingThreadList = new ArrayList<Thread>();
              this.loggingThreadMap.put(dbName, loggingThreadList);
              this.replicationGoingOnMap.put(dbName, false);
              if (REACTIVE) {
                ReactiveReplicationThread reactiveReplicThread = new ReactiveReplicationThread(createConnectionToLog(),
                  catalogConnection,
                  this,
                  Integer.parseInt(prop.getProperty(dbName + "_pinterval")),
                  vmId,
                  dbName,
                  Double.parseDouble(prop.getProperty(dbName + "_sla")));
                //reactiveReplicThread.start();
                reactiveReplicThreads.put(vmId + dbName, reactiveReplicThread);
              } else {
                QoSDBCForecaster qosdbcForecaster = new QoSDBCForecaster(createConnectionToLog(),
                  catalogConnection,
                  this,
                  Integer.parseInt(prop.getProperty(dbName + "_pinterval")),
                  vmId,
                  dbName,
                  Double.parseDouble(prop.getProperty(dbName + "_sla")));
                QoSDBCLogger logger = new QoSDBCLogger(logConnection, catalogConnection, this, vmId, dbName);
                //qosdbcForecaster.start();
                loggerThreads.put(vmId + dbName, logger);
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

  public void initService() {
    qosdbcLoadBalancer = new QoSDBCLoadBalancer(this);
  }

  private boolean IsValidDb(String dbName) {
    //return dbName.equals("twitter");
    return (!dbName.equals("information_schema") &&
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
        dbConnection.setSoTimeout(20 * 1000);
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
    int control = 0;
    for (QoSDBCConnectionProxy proxy : connectionProxies) {
      QoSDBCDatabaseProxy dao = proxy.getCurrentDAO();
      if (dao != null && dao.getDbName().equals(dbName)) {
        proxy.pause();
                /*
                if (REACTIVE) {
                    if(control == 0) {
                        reactiveReplicThreads.get(dao.getVmId() + dao.getDbName()).pauseThread();
                        control++;
                    }
                } else {
                    if (loggerThreads.containsKey(dao.getVmId() + dao.getDbName()))
                        loggerThreads.get(dao.getVmId() + dao.getDbName()).pauseThread();
                    if (forecastingThreads.containsKey(dao.getVmId() + dao.getDbName()))
                        forecastingThreads.get(dao.getVmId() + dao.getDbName()).pauseForecaster();
                }
                */
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
    int control = 0;
    for (QoSDBCConnectionProxy proxy : connectionProxies) {
      QoSDBCDatabaseProxy dao = proxy.getCurrentDAO();
      if (dao != null && dao.getDbName().equals(dbName)) {
        proxy.play();
                /*
                if (REACTIVE) {
                    if (control==0) {
                        reactiveReplicThreads.get(dao.getVmId() + dao.getDbName()).play();
                        control++;
                    }
                } else {
                    if (loggerThreads.containsKey(dao.getVmId() + dao.getDbName()))
                        loggerThreads.get(dao.getVmId() + dao.getDbName()).resumeThread();
                    if (forecastingThreads.containsKey(dao.getVmId() + dao.getDbName()))
                        forecastingThreads.get(dao.getVmId() + dao.getDbName()).resumeForecaster();
                }
                */
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
    String vmId = proxy.getVmId();
    String dbName = proxy.getDatabaseName();
    connectionProxies.remove(proxy);
    OutputMessage.println("[SERVICE]: connectionProxies.size() = " + connectionProxies.size());
    if (connectionProxies.size() == 0) {
      OutputMessage.println("[SERVICE]: All proxies FINISHED");
      this.qosdbcLoadBalancer.removeAllReplicas();
      //finishExecutor();
            /*
            if (REACTIVE) {
                if (reactiveReplicThreads.containsKey(vmId+dbName)) {
                    Thread forecasterThread = reactiveReplicThreads.get(vmId+dbName);
                    if (forecasterThread != null) forecasterThread.stop();
                    reactiveReplicThreads.remove(vmId+dbName);
                }
            } else {
                if (forecastingThreads.containsKey(vmId + dbName)) {
                    QoSDBCForecaster forecasterThread = forecastingThreads.get(vmId + dbName);
                    if (forecasterThread != null) forecasterThread.stop();
                    forecastingThreads.remove(vmId + dbName);
                }
            }*/
    }
  }

  /*
  public synchronized int updateLog() {
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
  */
  public QoSDBCLoadBalancer getLoadBalancer() {
    return this.qosdbcLoadBalancer;
  }

  public synchronized void startMonitoring(String vmId, String dbName) {
    if (REACTIVE) {
      ReactiveReplicationThread thread = reactiveReplicThreads.get(vmId + dbName);
      if (thread != null)
        if (!thread.isAlive()) thread.start();
    } else {
      QoSDBCLogger loggerThread = loggerThreads.get(vmId + dbName);
      if (loggerThread != null)
        if (!loggerThread.isAlive()) loggerThread.start();
      QoSDBCForecaster thread = forecastingThreads.get(vmId + dbName);
      if (thread != null)
        if (!thread.isAlive()) thread.start();
    }
  }

  public synchronized double getResponseTime(String dbName) {
    double rtSum = 0.0;
    int i = 0;
    double aux;
    for (QoSDBCConnectionProxy proxy : connectionProxies) {
      if (proxy == null) continue;
      if (proxy.getDatabaseName().equals(dbName)) {
        aux = proxy.getResponseTime();
        if (aux > 0.00000) {
          rtSum += aux;
          i++;
        }
      }
    }
    OutputMessage.println(dbName + " rtSum: " + rtSum + " count: " + (double) i);
    return rtSum / (double) i;
  }

  public synchronized void flushTempLog(String dbName) {
    List<String> temp = new ArrayList<String>();
    for (QoSDBCConnectionProxy proxy : connectionProxies) {
      if (proxy.getDatabaseName().equals(dbName)) {
        temp.addAll(proxy.getTempLog());
        //OutputMessage.println("Size of temp log: " + temp.size());
      }
    }
    executor.execute(new UpdateLogThread(temp, this.logConnection, dbName));
    //return updateLogThread;
  }

  public synchronized Thread flushTempLogBlocking(String dbName) {
    List<String> temp = new ArrayList<String>();
    for (QoSDBCConnectionProxy proxy : connectionProxies) {
      if (proxy.getDatabaseName().equals(dbName)) {
        temp.addAll(proxy.getTempLog());
        //OutputMessage.println("Size of temp log: " + temp.size());
      }
    }
    Thread thread = new Thread(new UpdateLogThread(temp, this.logConnection, dbName));
    this.loggingThreadMap.get(dbName).add(thread);
    return thread;
  }

  private void finishExecutor() {
    executor.shutdown();
    try {
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      OutputMessage.println("[Service]: " + " ERROR: On update log threads wait!");
    }
  }

  private Connection createConnectionToLog() {
    // Creates the connection with the Log's Service
    Connection logConnection = null;
    try {
      Class.forName("org.postgresql.Driver");
      logConnection = DriverManager.getConnection("jdbc:postgresql://" + "172.31.37.249" + ":" +
        "5432" + "/qosdbc-log", "postgres", "ufc123");
      qosdbc.commons.OutputMessage.println("qosdbc-coordinator: " + "connected to Log Service");
    } catch (SQLException ex) {
      qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
      System.exit(0);
    } catch (ClassNotFoundException ex) {
      qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
      System.exit(0);
    }
    return logConnection;
  }

  public synchronized Thread flushTempReplicaSyncLog(String dbName) {
    List<ReplicaSyncLogEntry> temp = new ArrayList<ReplicaSyncLogEntry>();
    for (QoSDBCConnectionProxy proxy : connectionProxies) {
      if (proxy.getDatabaseName().equals(dbName)) {
        temp.addAll(proxy.getReplicaSyncLogBuffer());
      }
    }
    Thread thread = new Thread(new UpdateReplicaSyncLogThread(temp, this.logConnection, dbName));
    return thread;
  }

  public void finishAllLoggingByNow(String dbName) {
    OutputMessage.println("[finishAllLoggingByNow] START");
    for (Thread t : loggingThreadMap.get(dbName)) {
      if (t != null) {
        if (t.getState() == Thread.State.RUNNABLE) {
          try {
            t.join();
          } catch (InterruptedException e) {
            OutputMessage.println("[finishAllLoggingByNow] ERROR: " + e.getMessage());
          }
        }
      }
    }
    OutputMessage.println("[finishAllLoggingByNow] FINISH");
  }

  public boolean IsDbUnderReplication(String dbName) {
    if (!replicationGoingOnMap.containsKey(dbName)) return false;
    return replicationGoingOnMap.get(dbName);
  }

  public void setDbReplicationStatus(String dbName, boolean isReplicating) {
    if (!replicationGoingOnMap.containsKey(dbName)) return;
    replicationGoingOnMap.put(dbName, isReplicating);
  }

  public synchronized void updateTenantRtAtVmId(String dbName, String vmId, double rt) {
    //Lock lock = null;
    Pair<String, String> key = new Pair<String, String>(dbName, vmId);
    if (!responseTimeMap.containsKey(key)) {
      //lock = new ReentrantLock(true);
      //responseTimeLockMap.put(key, lock);
      responseTimeCountMap.put(key, new AtomicInteger(0));
      responseTimeMap.put(key, new AtomicDouble(0d));
    }/* else {
            lock = responseTimeLockMap.get(key);
        }*/

    //lock.lock();
    double currentValue = responseTimeMap.get(key).get();
    int currentCount = responseTimeCountMap.get(key).get();
    double aux = currentCount + 1;
    currentValue = ((currentValue * (double) currentCount) + rt) / aux;
    currentCount++;
    responseTimeCountMap.get(key).set(currentCount);
    responseTimeMap.get(key).set((int) currentValue);
    //lock.unlock();
  }

  public synchronized double getTenantRtAtVmId(String dbName, String vmId) {
    //Lock lock = null;
    Pair<String, String> key = new Pair<String, String>(dbName, vmId);
        /*if (responseTimeLockMap.containsKey(key)) {
            lock = responseTimeLockMap.get(key);
        }
        if (lock != null) {*/
    if (responseTimeMap.containsKey(key)) {
      double rt = 0;
      //  lock.lock();
      rt = responseTimeMap.get(key).get();
      OutputMessage.println("[SERVICE] " + dbName + "/" + vmId + " rt: " + rt);
      //  lock.unlock();
      return rt;
    }
    OutputMessage.println("[SERVICE] WARNING" + dbName + "/" + vmId + " rt: 0.0");
    return 0d;
  }

  public synchronized void resetAllTenantRTBasedOnName(String dbName) {
    for (Map.Entry<Pair<String, String>, AtomicDouble> entry : responseTimeMap.entrySet()) {
      if (entry.getKey().getLeft().equals(dbName)) {
        //responseTimeLockMap.get(entry.getKey()).lock();
        responseTimeMap.get(entry.getKey()).set(0d);
        responseTimeCountMap.get(entry.getKey()).set(0);
        OutputMessage.println("[SERVICE] " + dbName + " rt: RESET");
        //responseTimeLockMap.get(entry.getKey()).unlock();
      }
    }
  }

  public void removeReplica(String dbName) {
    qosdbcLoadBalancer.removeReplica(dbName);
  }

  public void enableLogging(String dbName, boolean enable) {
    if (dbName.indexOf("ycsb") != -1) {
      for (QoSDBCConnectionProxy proxy : connectionProxies) {
        if (proxy.getDatabaseName().equals(dbName)) {
          proxy.enableLog(enable);
        }
      }
    }
  }
}