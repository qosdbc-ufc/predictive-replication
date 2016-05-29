package qosdbc.coordinator;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.TimeUnit;
import qosdbc.commons.DatabaseSystem;
import qosdbc.commons.OutputMessage;
import qosdbc.commons.jdbc.*;
import qosdbc.commons.jdbc.QoSDBCMessage.Response;
import qosdbc.commons.jdbc.QoSDBCMessage.Request;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class QoSDBCConnectionProxy extends Thread {

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

  /**
   *
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

    this.proxyId = System.currentTimeMillis();
    this.qosdbcService = qosdbcService;
    this.dbConnection = dbConnection;
    this.catalogConnection = catalogConnection;
    this.logConnection = logConnection;
    this.qosdbcLoadBalancer = qosdbcLoadBalancer;
    statementList = new Hashtable<Long, Statement>();
    resultSetList = new Hashtable<Long, ResultSet>();
  }

  /**
   *
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
        // adds this master if it is not registered yet
        String replicaVmId;
        if (IsValidTenant(dbName)) {
          qosdbcLoadBalancer.addMaster(vmId);
          // gets the replica to process the current request
          replicaVmId = qosdbcLoadBalancer.targetReplica(catalogConnection, dbName, vmId);
        } else {
          replicaVmId = vmId;
        }
        switch (dbmsType) {
          case DatabaseSystem.TYPE_MYSQL: {
            if (dao != null) {
              dao = new QoSDBCDatabaseProxy("com.mysql.jdbc.Driver", "jdbc:mysql://" + replicaVmId + ":3306/" + dbName, dbName, "root", "ufc123", replicaVmId, dao.getConnection().getAutoCommit());
            } else {
              dao = new QoSDBCDatabaseProxy("com.mysql.jdbc.Driver", "jdbc:mysql://" + replicaVmId + ":3306/" + dbName, dbName, "root", "ufc123", replicaVmId, true);
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
          //OutputMessage.println(dbName + " in " + replicaVmId + " is connected");
          return dao;
        } else {
          OutputMessage.println(dbName + " in " + replicaVmId + " is not connected");
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
   *
   * @param statementId
   * @return
   */
  private Statement getStatement(long statementId) {
    return statementList.get(statementId);
  }

  /**
   *
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
    pause = true;
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

        //  synchronized (this) { // SYNCHRONIZED
        if (pause && lastRequestWasCommitOrRollback) {
          try {
            dao.commit();
            OutputMessage.println("[" + proxyId + "]: PAUSED");
            flagMigration = false;
            wait();
            if (inMigration) {
              flagMigration = true;
            }
            OutputMessage.println("[" + proxyId + "]: PLAYED");
          } catch (InterruptedException ex) {
            OutputMessage.println("[" + proxyId + "]: Error " + ex.getMessage());
          }
        }
        // } // SYNCHRONIZED

        if (changeDAO) {
          try {
            boolean autoCommit = dao.getConnection().getAutoCommit();
            dao = getDatabaseProxy(databaseName);
            //OutputMessage.println("[" + proxyId + "]: Trying to get a dao...");
            //OutputMessage.println("[" + proxyId + "]: GOT IT");
            Connection connection = dao.getConnection();
            connection.setAutoCommit(autoCommit);
            changeDAO = false;
            // Update the Statments to new connection
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
        }

        // synchronized (this) { // SYNCHRONIZED
        // reads the request from the stream
        Request msg = Request.parseDelimitedFrom(inputStream);
        Response.Builder response = Response.newBuilder();

        long startTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        //OutputMessage.println("[" + proxyId + "]: " + "CODE: " + msg.getCode()
        //        + " COMMAND: " + msg.getCommand() + " DATABASE: " + msg.getDatabase());
        switch (msg.getCode()) {
          case RequestCode.SQL_CONNECTION_CREATE: {
            if (dao != null && dao.isActive()) {
              dao.close();
            }
            dao = getDatabaseProxy(msg.getDatabase());
            // OutputMessage.println("[" + proxyId + "]: GOT IT");
            response.setState(RequestCode.STATE_SUCCESS);
            break;
          }
          case RequestCode.SQL_CONNECTION_CLOSE: {
            dao.rollback();
            if (dao != null && dao.isActive()) {
              dao.close();
            }
            closeConnection = true;
            response.setState(RequestCode.STATE_SUCCESS);
            break;
          }
          case RequestCode.SQL_STATEMENT_CREATE: {
            response.setState(RequestCode.STATE_SUCCESS);
            try {
              if (dao.getConnection().getAutoCommit()) {
                // changeDAO = true;
              }
              Statement statement = dao.getConnection().createStatement(); // AO MUDAR O DAO, ESTAMOS PERDENDO OS STATEMENTS
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
              statementList.remove(Long.parseLong(msg.getParameters().get("statementId")));
              //OutputMessage.println("[" + proxyId + "]: " + "SUCCESS: " + "SQL_STATEMENT_CLOSE");
            } catch (SQLException ex) {
              OutputMessage.println("[" + proxyId + "]: " + "FAILURE: " + "SQL_STATEMENT_CLOSE");
              response.setState(RequestCode.STATE_FAILURE);
            }
            break;
          }
          case RequestCode.SQL_RESULTSET_CREATE: {
            try {
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
              resultSetList.remove(Long.parseLong(msg.getParameters().get("resultSetId")));
            } catch (SQLException ex) {
              OutputMessage.println("[" + proxyId + "]: " + "FAILURE: " + "SQL_RESULTSET_CLOSE");
              response.setState(RequestCode.STATE_FAILURE);
            }
            break;
          }
          case RequestCode.SQL_COMMIT: {
            dao.commit();
            if (!dao.getConnection().getAutoCommit()) {
              //changeDAO = true;
            }
            response.setState(RequestCode.STATE_SUCCESS);
            //dao.close();
            break;
          }
          case RequestCode.SQL_ROLLBACK: {
            //OutputMessage.println("[" + proxyId + "]: " + " ROLLBACK REQUESTED ");
            dao.rollback();
            if (!dao.getConnection().getAutoCommit()) {
              //changeDAO = true;
            }
            response.setState(RequestCode.STATE_SUCCESS);
            //dao.close();
            break;
          }
          case RequestCode.SQL_UPDATE: {
            int result;
            result = dao.update(msg.getCommand(), getStatement(Long.parseLong(msg.getParameters().get("statementId"))));
            if (result == -1) {
              //@gambiarra
              //result = 1; // TO DO ERROR IN CHANGE CONNECTION
              //pw.println(msg.getCommand());
              OutputMessage.println("[" + proxyId + "]: " + "FAILURE: SQL_UPDATE << " + msg.getCommand());
            } else {
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
              // dao = getDatabaseProxy(msg.getDatabase());
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
        response.setStartTime(TimeUnit.NANOSECONDS.toMillis(startTime));
        response.setFinishTime(TimeUnit.NANOSECONDS.toMillis(finishTime));

        if (msg.getCommand() != null && (msg.getCode() == RequestCode.SQL_UPDATE || msg.getCode() == RequestCode.SQL_RESULTSET_CREATE || msg.getCode() == RequestCode.SQL_COMMIT || msg.getCode() == RequestCode.SQL_ROLLBACK)) {
          if (msg.getCode() == RequestCode.SQL_ROLLBACK) {
            try {
              Statement statement = logConnection.createStatement();
              int result = statement.executeUpdate("DELETE FROM sql_log WHERE transaction_id = " + msg.getTransactionId());
              OutputMessage.println("[" + proxyId + "]: # OF ROWS DELETED: " + result);
              statement.close();
            } catch (SQLException ex) {
              OutputMessage.println("[" + proxyId + "]: ERROR while rollbacking on log!");
            }
          } else {
            log(msg.getCommand(), dao.getVmId(), dao.getDbName(), msg.getCode(), (finishTime - startTime), msg.getSlaResponseTime(), msg.getConnectionId(), msg.getTransactionId(), response.getAffectedRows(), flagMigration);
          }
        }

        // sends response through the stream
        response.build().writeDelimitedTo(outputStream);
        outputStream.flush();

        if (msg.getCode() == RequestCode.SQL_ROLLBACK ||
            msg.getCode() == RequestCode.SQL_COMMIT) {
          this.lastRequestWasCommitOrRollback = true;
        } else {
          this.lastRequestWasCommitOrRollback = false;
        }

        if (closeConnection) {
          //OutputMessage.println("[" + proxyId + "]: Closing proxy connection");
          if (dbConnection != null) {
            try {
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
    dao.close();
    qosdbcService.removeConnectionProxy(this);
    // OutputMessage.println("[" + proxyId + "]: Proxy connection ended");
    //pw.close();
  }

  /**
   *
   * @param resultSet
   * @return
   */
  private static List<Response.Row> transformResultSetToRowList(ResultSet resultSet) {
    List<Response.Row> resultSetList = new ArrayList<Response.Row>();
    try {
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
    }
    return null;
  }

  /**
   *
   * @param sql
   */
  private void log(String sql, String vmId, String dbName, int requestCode, long responseTime, long slaResponseTime, long connectionId, long transactionId, long affectedRows, boolean inMigration) {
    if (sql != null) {
      sql = sql.replaceAll("[\']", "''");
    }
    String sqlLog = ""
        + "INSERT INTO sql_log(\"time\", vm_id, db_name, time_local, sql, sql_type, response_time, sla_response_time, sla_violated, connection_id, transaction_id, affected_rows, in_migration) "
        + "VALUES (now(), '" + vmId + "', '" + dbName + "', " + System.currentTimeMillis() + ", '" + sql + "', " + requestCode + ", " + responseTime + ", " + slaResponseTime + ", " + (responseTime > slaResponseTime) + ", " + connectionId + ", " + transactionId + ", " + affectedRows + ", " + inMigration + ")";
    try {
      Statement statement = logConnection.createStatement();
      statement.executeUpdate(sqlLog);
      statement.close();
    } catch (SQLException ex) {
      OutputMessage.println("ERROR: " + ex.getMessage());
    }
  }

  public QoSDBCDatabaseProxy getCurrentDAO() {
    return dao;
  }

  private boolean IsValidTenant(String dbName) {
    return !dbName.equals("information_schema")
        && !dbName.equals("mysql")
        && !dbName.equals("performance_schema");
  }
}
