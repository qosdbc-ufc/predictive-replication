package qosdbc.coordinator;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import qosdbc.commons.DatabaseSystem;
import qosdbc.commons.OutputMessage;
import qosdbc.commons.PendingRequest;
import qosdbc.commons.jdbc.*;
import qosdbc.commons.jdbc.QoSDBCMessage.Response;
import qosdbc.commons.jdbc.QoSDBCMessage.Request;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class QoSDBCConnectionProxy extends Thread {

  private final static String DELIMITER = "\"|\"";
  private QoSDBCService qosdbcService = null;
  private QoSDBCLoadBalancer qosdbcLoadBalancer = null;
  private Socket dbConnection = null;
  private Connection catalogConnection = null;
  private Connection logConnection = null;
  private QoSDBCDatabaseProxy dao = null;
  private Hashtable<Long, Statement> statementList = null;
  private Hashtable<Long, ResultSet> resultSetList = null;
  private long proxyId;
  private String databaseName;
  private boolean changeDAO = false;
  private boolean pause = false;
  private boolean lastRequestWasCommitOrRollback = false;
  private boolean inMigration = false;
  private boolean flagMigration = false;
  private List<String> tempLog;
  private boolean monitoringStarted = false;
  private String vmId;
  private Lock lock = null;
  private Lock replicaSyncLock = null;
  private List<ReplicaSyncLogEntry> replicaSyncLogBuffer = null;


  private long responseTimeSum = 0;
  private long responseTimeCount = 0;

  //private AtomicLong responseTimeSum;
  //private AtomicLong responseTimeCount;

  /**
   * @param qosdbcService
   * @param dbConnection
   * @param catalogConnection
   * @param logConnection*
   */
  public QoSDBCConnectionProxy(QoSDBCService qosdbcService,
                               Socket dbConnection,
                               Connection catalogConnection,
                               Connection logConnection,
                               QoSDBCLoadBalancer qosdbcLoadBalancer) {

    this.proxyId = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) + ThreadLocalRandom.current().nextInt(1, 1000000 + 1);
    this.qosdbcService = qosdbcService;
    this.dbConnection = dbConnection;
    this.catalogConnection = catalogConnection;
    this.logConnection = logConnection;
    this.qosdbcLoadBalancer = qosdbcLoadBalancer;
    statementList = new Hashtable<Long, Statement>();
    resultSetList = new Hashtable<Long, ResultSet>();
    tempLog = Collections.synchronizedList(new ArrayList<String>());
    replicaSyncLogBuffer = new ArrayList<ReplicaSyncLogEntry>();
    this.lock = new ReentrantLock(true);
    replicaSyncLock = new ReentrantLock(true);
    //this.responseTimeSum = new AtomicLong(0);
    //this.responseTimeCount = new AtomicLong(0);
  }

  /**
   * @param dbName
   * @return
   */
  private QoSDBCDatabaseProxy getDatabaseProxy(String dbName) {
    try {
      boolean foundDatabase = false;
      Statement statement = catalogConnection.createStatement();
      ResultSet resultSet = statement.executeQuery("SELECT \"time\", vm_id, db_name, dbms_type from db_active where db_name = '" + dbName + "'");
      while (resultSet.next()) {
        int dbmsType = resultSet.getInt("dbms_type");
        String vmId = resultSet.getString("vm_id");
        this.vmId = vmId;
        switch (dbmsType) {
          case DatabaseSystem.TYPE_MYSQL: {
            if (dao != null) {
              dao = new QoSDBCDatabaseProxy("com.mysql.jdbc.Driver", "jdbc:mysql://" + vmId + ":3306/" + dbName + "?dontTrackOpenResources=true", dbName, "root", "ufc123", vmId, dao.getConnection().getAutoCommit());
            } else {
              dao = new QoSDBCDatabaseProxy("com.mysql.jdbc.Driver", "jdbc:mysql://" + vmId + ":3306/" + dbName + "?dontTrackOpenResources=true", dbName, "root", "ufc123", vmId, true);
            }
            foundDatabase = true;
            break;
          }
          case DatabaseSystem.TYPE_POSTGRES: {
            //dao = new QoSDBCDatabaseProxy("org.postgresql.Driver", "jdbc:postgresql://" + replicaVmId + ":5432/" + dbName, dbName, "postgres", "ufc123", replicaVmId);
            foundDatabase = true;
            break;
          }
        }
        if (dao.isActive() && foundDatabase) {
          databaseName = dbName;
          qosdbcLoadBalancer.addTenant(this.proxyId, dbName, dao);
          OutputMessage.println(dbName + " in " + vmId + " is connected");
          return dao;
        } else {
          OutputMessage.println(dbName + " in " + vmId + " is not connected");
          OutputMessage.println("It will try again...");
          //System.exit(0);
        }
      }

      resultSet.close();
      statement.close();

    } catch (SQLException ex) {
      OutputMessage.println("ERROR: " + ex.getMessage());
    }
    return null;
  }

  /**
   * @param statementId
   * @return
   */
  private Statement getStatement(long statementId) {
    return statementList.get(statementId);
  }

  /**
   * @param resultSetId
   * @return
   */
  private ResultSet getResultSet(long resultSetId) {
    return resultSetList.get(resultSetId);
  }

  /**
   * Get the database name
   *
   * @return
   */
  public String getDatabaseName() {
    return databaseName;
  }

  public void changeDatabaseConnection() {
    changeDAO = true;
  }

  public void pause() {
    synchronized (this) {
      pause = true;
    }
  }

  public void play() {
    synchronized (this) {
      pause = false;
      notify();
    }
  }

  public void setInMigration(boolean value) {
    inMigration = value;
  }

  @Override
  public void run() {
    OutputMessage.println("[" + proxyId + "]: Proxy connection starting");
    ObjectOutputStream outputStream = null;
    ObjectInputStream inputStream = null;
    boolean proceed = true;
    try {
      outputStream = new ObjectOutputStream(new BufferedOutputStream(dbConnection.getOutputStream()));
      outputStream.flush();
      inputStream = new ObjectInputStream(new BufferedInputStream((dbConnection.getInputStream())));
    } catch (IOException ex) {
      OutputMessage.println("[" + proxyId + "]: Closing proxy connection");
      proceed = false;
    }
    OutputMessage.println("[" + proxyId + "]: Proxy connection started");

    boolean closeConnection = false;
    while (proceed && dbConnection != null && dbConnection.isConnected()) {
      try {
        synchronized (this) { // SYNCHRONIZED
          if (pause && lastRequestWasCommitOrRollback) {
            try {
              dao.commit();
              OutputMessage.println("[" + proxyId + "]: PAUSED");
              flagMigration = false;
              wait();
              if (inMigration) {
                flagMigration = true;
              }
              changeDAO = false;
              OutputMessage.println("[" + proxyId + "]: PLAYED");
            } catch (InterruptedException ex) {
              OutputMessage.println("[" + proxyId + "]: Error " + ex.getMessage());
            }
          }
        } // SYNCHRONIZED

        if (changeDAO) {
          long changeDAOStart = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
          try {
            boolean autoCommit = dao.getConnection().getAutoCommit();
            //OutputMessage.println("[" + proxyId + "]: changeDAO ");
            dao = qosdbcLoadBalancer.getTarget(this.proxyId, databaseName);
            //OutputMessage.println("[" + proxyId + "]: DAO: " + dao.getVmId());
            if (dao == null) {
              OutputMessage.println("[" + proxyId + "]: [WARNING]: getTarget returned NULL");
              System.exit(-1);
            }
            Connection connection = dao.getConnection();
            if (connection == null) {
              // @gambi
              OutputMessage.println("[" + proxyId + "]: [WARNING]: changeDAO dao.getConnection() NULL");
              break;
            }
            connection.setAutoCommit(autoCommit);
            changeDAO = false;
            // Update the Statements to new connection
            Enumeration<Long> statementIdList = statementList.keys();
            while (statementIdList.hasMoreElements()) {
              Long statementId = statementIdList.nextElement();
              Statement statement = connection.createStatement();
              statementList.put(statementId, statement);
              //OutputMessage.println("[" + proxyId + "]: Statement ID " + statementId);
            }
          } catch (SQLException ex) {
            OutputMessage.println("[" + proxyId + "]: changeDAO " + ex.getMessage());
            System.exit(0);
          }
          long changeDAOFinish = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
          if (changeDAOFinish - changeDAOStart > 10000) {
            OutputMessage.println("[" + proxyId + "]: [WARNING] changeDAO taking too LONG ");
          }
        }

        // synchronized (this) { // SYNCHRONIZED
        // reads the request from the stream
        //System.out.println("[" + proxyId + "]: NEW MESSAGE RECEIVED 1");
        //OutputMessage.println("[" + proxyId + "]: NEW MESSAGE RECEIVED 1");
        Request msg = Request.parseDelimitedFrom(inputStream);
        //System.out.println("[" + proxyId + "]: NEW MESSAGE RECEIVED 2 " + msg.getCommand());
        //OutputMessage.println("[" + proxyId + "]: NEW MESSAGE RECEIVED 2 " + msg.getCommand() + " << " + msg.getCode());
        Response.Builder response = Response.newBuilder();
        if (msg == null) {
          response.setState(RequestCode.STATE_SUCCESS);
          response.build().writeDelimitedTo(outputStream);
          outputStream.flush();
          OutputMessage.println("[" + proxyId + "]: NEW MESSAGE FAILED");
          continue;
        }
        if (!IsValidTenant(msg.getDatabase())) {
          response.setState(RequestCode.STATE_SUCCESS);
          response.build().writeDelimitedTo(outputStream);
          outputStream.flush();
          OutputMessage.println("[" + proxyId + "]: IsValidTenant NEW MESSAGE FAILED");
          continue;
        }
        long startTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        long startSyncReplicas = 0;
        long finishSyncReplicas = 0;
        //OutputMessage.println("[" + proxyId + "]: " + "CODE: " + msg.getCode()
        //        + " COMMAND: " + msg.getCommand() + " DATABASE: " + msg.getDatabase());

        switch (msg.getCode()) {
          case RequestCode.SQL_CONNECTION_CREATE: {
            if (dao != null && dao.isActive()) {
              OutputMessage.println("[" + proxyId + "]: close connection on create new!");
              dao.close();
            }
            dao = getDatabaseProxy(msg.getDatabase());
            //OutputMessage.println("[" + proxyId + "]: GOT IT");
            response.setState(RequestCode.STATE_SUCCESS);
            break;
          }
          case RequestCode.SQL_CONNECTION_CLOSE: {
            //qosdbcService.flushTempLogBlocking(this.dao.getDbName());
            dao.rollback();
            if (dao != null && dao.isActive()) {
              //dao.close();
              qosdbcLoadBalancer.removeTenant(this.proxyId);
            }
            closeConnection = true;
            response.setState(RequestCode.STATE_SUCCESS);
            break;
          }
          case RequestCode.SQL_STATEMENT_CREATE: {
            response.setState(RequestCode.STATE_SUCCESS);
            try {
              Statement statement = dao.getConnection().createStatement(); // AO MUDAR O DAO, ESTAMOS PERDENDO OS STATEMENTS
              //OutputMessage.println("[" + proxyId + "]: DAO-SC: " + dao.getVmId() + "/" + dao.getDbName() + "/" + dao.getId());
              statement.setEscapeProcessing(false);
              statementList.put(Long.parseLong(msg.getParameters().get("statementId")), statement);
              //daoList.put(dao.getId(), Long.parseLong(msg.getParameterValue("statementId").toString()));
              //OutputMessage.println("[" + proxyId + "]: " + "SUCCESS: " + "SQL_STATEMENT_CREATE");
            } catch (SQLException ex) {
              OutputMessage.println("[" + proxyId + "]: " + "FAILURE: " + "SQL_STATEMENT_CREATE");
              response.setState(RequestCode.STATE_FAILURE);
            }
            break;
          }
          case RequestCode.SQL_STATEMENT_CLOSE: {
            response.setState(RequestCode.STATE_SUCCESS);
            try {
              getStatement(Long.parseLong(msg.getParameters().get("statementId"))).close();
              Statement statement = statementList.remove(Long.parseLong(msg.getParameters().get("statementId")));
              //OutputMessage.println("[" + proxyId + "]: " + "SUCCESS: " + "SQL_STATEMENT_CLOSE");
            } catch (SQLException ex) {
              OutputMessage.println("[" + proxyId + "]: " + "FAILURE: " + "SQL_STATEMENT_CLOSE");
              response.setState(RequestCode.STATE_FAILURE);
            }
            break;
          }
          case RequestCode.SQL_RESULTSET_CREATE: {
            try {

              if (dao.getConnection().getAutoCommit()) {
                changeDAO = true;
              }

              // TODO(Serafim): if it returns false redirect the request to proper host

              startSyncReplicas = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
              ApplyPendingUpdates(msg.getDatabase(), dao.getVmId(), msg.getTransactionId());
              finishSyncReplicas = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

              Statement statement = getStatement(Long.parseLong(msg.getParameters().get("statementId")));
              ResultSet resultSet = null;
              resultSet = statement.executeQuery(msg.getCommand());
              List<Response.Row> resultSetList = transformResultSetToRowList(resultSet);
              response.addAllResultSet(resultSetList);
              this.resultSetList.put(Long.parseLong(msg.getParameters().get("resultSetId")), resultSet);
              response.setState(RequestCode.STATE_SUCCESS);
              response.setAffectedRows(resultSetList.size());
              if (resultSetList.isEmpty()) {
                //OutputMessage.println("[" + proxyId + "]: " + "FAILURE: SQL_RESULTSET_CREATE << " + msg.getCommand());
              } else {
              }
            } catch (SQLException ex) {
              //pw.println(msg.getCommand());
              OutputMessage.println("[" + proxyId + "]: " + "SQLException: " + msg.getCommand());
              OutputMessage.println("[" + proxyId + "]: " + "SQLException: " + ex.getMessage());
              response.setState(RequestCode.STATE_FAILURE);
            }
            break;
          }
          case RequestCode.SQL_RESULTSET_CLOSE: {
            response.setState(RequestCode.STATE_SUCCESS);
            try {
              getResultSet(Long.parseLong(msg.getParameters().get("resultSetId"))).close();
              ResultSet rs = resultSetList.remove(Long.parseLong(msg.getParameters().get("resultSetId")));

            } catch (SQLException ex) {
              OutputMessage.println("[" + proxyId + "]: " + "FAILURE: " + "SQL_RESULTSET_CLOSE");
              response.setState(RequestCode.STATE_FAILURE);
            }
            break;
          }
          case RequestCode.SQL_COMMIT: {
            dao.commit();
            //OutputMessage.println("[" + proxyId + "]: DAO-C: " + dao.getVmId() + "/" + dao.getDbName() + "/" + dao.getId());
            if (!dao.getConnection().getAutoCommit()) {
              changeDAO = true;
            }
            response.setState(RequestCode.STATE_SUCCESS);
            break;
          }
          case RequestCode.SQL_ROLLBACK: {
            //OutputMessage.println("[" + proxyId + "]: " + " ROLLBACK REQUESTED ");
            dao.rollback();
            if (!dao.getConnection().getAutoCommit()) {
              changeDAO = true;
            }
            response.setState(RequestCode.STATE_SUCCESS);
            break;
          }
          case RequestCode.SQL_UPDATE: {
            int result;

            startSyncReplicas = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            ApplyPendingUpdates(msg.getDatabase(), dao.getVmId(), msg.getTransactionId());
            finishSyncReplicas = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

            result = dao.update(msg.getCommand(), getStatement(Long.parseLong(msg.getParameters().get("statementId"))));
            if (result == -1) {
              //@gambiarra
              result = 1; // TO DO ERROR IN CHANGE CONNECTION
              //pw.println(msg.getCommand());
              OutputMessage.println("[" + proxyId + "]: " + "FAILURE: SQL_UPDATE << " + msg.getCommand());
            } else {
              QoSDBCService.consistencyService.addPendingUpdate(msg.getDatabase(), dao.getVmId(), msg);
            }
            response.setResultObject(result);
            response.setState(RequestCode.STATE_SUCCESS);
            response.setAffectedRows(result);
            break;
          }
          case RequestCode.CONNECTION_CHANGE_AUTOCOMMIT: {
            try {
              //if (dao != null && dao.isActive()) {
              //    dao.close();
              //}
              qosdbcLoadBalancer.removeTenant(this.proxyId);
              dao = getDatabaseProxy(msg.getDatabase());
              Connection connection = dao.getConnection();
              boolean newValue = Boolean.parseBoolean(msg.getParameters().get("autoCommit"));
              connection.setAutoCommit(newValue);
              dao.setConnection(connection);
              response.setState(RequestCode.STATE_SUCCESS);
            } catch (SQLException ex) {
              OutputMessage.println("[" + proxyId + "]: " + "FAILURE: CONNECTION_CHANGE_AUTOCOMMIT");
              response.setState(RequestCode.STATE_FAILURE);
            }
            break;
          }
        }

        long finishTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        response.setStartTime(startTime);
        response.setFinishTime(finishTime);

        if (msg.getCommand() != null && (msg.getCode() == RequestCode.SQL_UPDATE ||
            msg.getCode() == RequestCode.SQL_RESULTSET_CREATE ||
            msg.getCode() == RequestCode.SQL_COMMIT ||
            msg.getCode() == RequestCode.SQL_ROLLBACK)) {
          if (msg.getCode() == RequestCode.SQL_ROLLBACK) {
            try {
              Statement statement = logConnection.createStatement();
              int result = statement.executeUpdate("DELETE FROM sql_log WHERE transaction_id = " + msg.getTransactionId());
              if (result > 0) OutputMessage.println("[" + proxyId + "]: # OF ROWS DELETED: " + result);
              statement.close();
            } catch (SQLException ex) {
              OutputMessage.println("[" + proxyId + "]: ERROR while rollbacking on log!");
            }
          } else {
            String command = msg.getCommand();
            if (msg.getCode() == RequestCode.SQL_COMMIT) {
              command = "COMMIT";
            } else if (msg.getCode() == RequestCode.SQL_ROLLBACK) {
              command = "ROLLBACK";
            }
            synchronized (this) {
              if (!monitoringStarted) {
                this.qosdbcService.startMonitoring(this.vmId, this.databaseName);
                monitoringStarted = true;
              }
            }

            long replicaSyncTime = (finishSyncReplicas - startSyncReplicas);
            AddEntryToReplicaSyncLog(startSyncReplicas, databaseName, replicaSyncTime);
            lock.lock();
              responseTimeSum += ((finishTime - startTime) - replicaSyncTime);
              responseTimeCount++;
            lock.unlock();

            long startLong = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            log(command, dao.getVmId(), dao.getDbName(), msg.getCode(), ((finishTime - startTime) - replicaSyncTime),
             msg.getSlaResponseTime(), msg.getConnectionId(), msg.getTransactionId(),
                    response.getAffectedRows(), flagMigration);
            long finishLong = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            if (finishLong - startLong > 30000) {
              OutputMessage.println("[" + proxyId + "]: WARNING: Logging taking too long");
            }
          }
        }

        if (msg.getCode() == RequestCode.SQL_ROLLBACK ||
            msg.getCode() == RequestCode.SQL_COMMIT) {
          this.lastRequestWasCommitOrRollback = true;
        } else {
          this.lastRequestWasCommitOrRollback = false;
        }

        // sends response through the stream
        response.build().writeDelimitedTo(outputStream);
        outputStream.flush();


        if (closeConnection) {
          //OutputMessage.println("[" + proxyId + "]: Closing proxy connection");
          if (dbConnection != null) {
            try {
              if (dao.isActive()) dao.close(); //@gambi
              dbConnection.close();
            } catch (IOException ex1) {
              dbConnection = null;
            }
          }
          break;
        }

        // } // SYNCHRONIZED
      } catch (IOException ex) {
        OutputMessage.println("[" + proxyId + "] ERROR: Closing proxy connection");
        ex.printStackTrace();
        if (dbConnection != null) {
          try {
            dbConnection.close();
          } catch (IOException ex1) {
            dbConnection = null;
          }
        }
        break;
      } catch (SQLException ex) {
        OutputMessage.println("[" + proxyId + "]: ERROR:  SQLException");
        ex.printStackTrace();
      } // TRY
    } // WHILE
    // Close all database connections
    //qosdbcLoadBalancer.removeReplica(this.databaseName, dao);
    //dao.close();
    qosdbcService.removeConnectionProxy(this);
    try {
      outputStream.close();
      inputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    OutputMessage.println("[" + proxyId + "]: Proxy connection ended");
    //pw.close();
  }

  /**
   * @param resultSet
   * @return
   */
  private List<Response.Row> transformResultSetToRowList(ResultSet resultSet) {
    List<Response.Row> resultSetList = null;
    try {
      resultSetList = new ArrayList<Response.Row>(resultSet.getFetchSize());
      java.sql.ResultSetMetaData metaData = resultSet.getMetaData();
      while (resultSet.next()) {
        Response.Row.Builder row = Response.Row.newBuilder();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          Response.Row.Column.Builder column = Response.Row.Column.newBuilder();
          column.setColumnIndex(i);
          column.setColumnLabel(metaData.getColumnLabel(i));
          column.setColumnValue(String.valueOf(resultSet.getObject(metaData.getColumnLabel(i))));
          row.addColumnList(column);
        }
        resultSetList.add(row.build());
      }
      return resultSetList;
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
    return null;
  }

  /**
   * @param sql
   */
  private void log(String sql, String vmId, String dbName, int requestCode, long responseTime, long slaResponseTime,
                   long connectionId, long transactionId, long affectedRows, boolean inMigration) {

    if (sql != null) {
      sql = sql.replaceAll("[\']", "''");
      sql = sql.replaceAll("\"", "\\\\\""); // gambiarra para wikipedia em csv
    }


    //String sqlLog = "(" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) + " , '" + vmId + "', '" + dbName + "', " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) + ", '" + sql + "', " + requestCode + ", " + responseTime + ", " + slaResponseTime + ", " + (responseTime > slaResponseTime) + ", " + connectionId + ", " + transactionId + ", " + affectedRows + ", " + inMigration + ")";
    String sqlLog2 = "\"" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) + DELIMITER + vmId + DELIMITER + dbName + DELIMITER + TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) + DELIMITER
        + sql + DELIMITER + requestCode + DELIMITER + responseTime + DELIMITER + slaResponseTime + DELIMITER + (responseTime > slaResponseTime) + DELIMITER + connectionId + DELIMITER + transactionId +
        DELIMITER + affectedRows + DELIMITER + inMigration + "\"";

      tempLog.add(sqlLog2);
      //OutputMessage.println("Added(" + tempLog.size() +") " + sqlLog);

  }

  public QoSDBCDatabaseProxy getCurrentDAO() {
    return dao;
  }

  private boolean IsValidTenant(String dbName) {
    return //!dbName.equals("information_schema")
        !dbName.equals("mysql")
            && !dbName.equals("performance_schema");
  }


  public double getResponseTime() {
    double rt = 0.0;
    long sum = 0;
    long count = 0;
    lock.lock();
      sum = responseTimeSum;
      count = responseTimeCount;
      responseTimeSum = 0;
      responseTimeCount = 0;
    lock.unlock();

    if (count == 0) {
      OutputMessage.println("[WARNING] " + this.databaseName + " count == 0");
      return 0.0;
    }
    //OutputMessage.println(databaseName + " proxuSUM: " + (double)sum + " count: " + (double)count);
    rt = (double) sum / (double) count;

    return rt;
  }

  public synchronized List<String> getTempLog() {
    List<String> copy;
    copy = new ArrayList<String>(tempLog.size());
    copy.addAll(tempLog);
    tempLog.clear();
    return copy;
  }

  public boolean ApplyPendingUpdates(String dbName, String vmId, long time) {
    // get pending updates that arrived before the current read request

    ArrayList<PendingRequest> pendingUpdates = QoSDBCService.consistencyService.getPendingRequestFor(dbName, vmId, time);
    if (pendingUpdates == null) {
      OutputMessage.println("[ApplyPendingUpdates]: pendingUpdates NULL");
      return false;
    }
    int result = 0;
    Statement state = null;
    try {
      state = dao.getConnection().createStatement();
      if (state == null) {
        OutputMessage.println("[ApplyPendingUpdates]: STATE NULL");
        return true;
      }
      if (dao == null) {
        OutputMessage.println("[ApplyPendingUpdates]: DAO NULL");
        return true;
      }
      for (int i=0; i<pendingUpdates.size(); ++i) {
        PendingRequest update = pendingUpdates.get(i);
        if (update == null) {
          OutputMessage.println("[ApplyPendingUpdates]: PendingRequest NULL");
          return true;
        }
        state.addBatch(update.getCommand());
      }
      int[] res = state.executeBatch();
      state.close();

      //if ((finish - startTime) > 1000)
      //  OutputMessage.println("[ApplyPendingUpdates]: DONE in " + (finish - startTime));
    } catch(Exception ex) {
      OutputMessage.println("[ApplyPendingUpdates]: " + ex.getMessage());
    }
    return true;
  }

  public String getVmId() {
    return vmId;
  }

  public void AddEntryToReplicaSyncLog(long time, String dbName, long syncTime) {
    replicaSyncLock.lock();
      this.replicaSyncLogBuffer.add(new ReplicaSyncLogEntry(time, dbName, syncTime));
    replicaSyncLock.unlock();
  }

  public List<ReplicaSyncLogEntry> getReplicaSyncLogBuffer() {
    List<ReplicaSyncLogEntry> copy = null;
    replicaSyncLock.lock();
      copy = new ArrayList<ReplicaSyncLogEntry>(replicaSyncLogBuffer.size());
      copy.addAll(replicaSyncLogBuffer);
      replicaSyncLogBuffer.clear();
    replicaSyncLock.unlock();
    return copy;
  }
}