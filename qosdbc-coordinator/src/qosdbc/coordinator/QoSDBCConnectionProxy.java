/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import qosdbc.commons.DatabaseSystem;
import qosdbc.commons.OutputMessage;
import qosdbc.commons.jdbc.Column;
import qosdbc.commons.jdbc.Request;
import qosdbc.commons.jdbc.RequestCode;
import qosdbc.commons.jdbc.Response;
import qosdbc.commons.jdbc.Row;

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
    //private Map<Long, Long> daoList = null;
    private long proxyId;
    private String databaseName;
    private boolean changeDAO = false;
    private boolean pause = false;
    private Request lastRequest = null;
    private boolean inMigration = false;
    private boolean flagMigration = false;
    
    //private PrintWriter pw;

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
        //daoList = new HashMap<Long, Long>();
        /*
        try {
            pw = new PrintWriter("/home/leoomoreira/sql_" + System.currentTimeMillis() + ".txt");
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        */ 
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
                qosdbcLoadBalancer.addMaster(vmId);
                // gets the replica to recive the workload
                String replicaVmId = qosdbcLoadBalancer.targetReplica(catalogConnection, dbName, vmId);
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
            outputStream = new ObjectOutputStream((dbConnection.getOutputStream()));
            inputStream = new ObjectInputStream((dbConnection.getInputStream()));
        } catch (IOException ex) {
            OutputMessage.println("[" + proxyId + "]: Closing proxy connection");
            proceed = false;
        }
        OutputMessage.println("[" + proxyId + "]: Proxy connection started");

        boolean closeConnection = false;
        while (proceed && dbConnection != null && dbConnection.isConnected()) {
            try {
                
                synchronized (this) { // SYNCHRONIZED
                    if (pause && (lastRequest != null
                            && (lastRequest.getCode() == RequestCode.SQL_COMMIT
                            || lastRequest.getCode() == RequestCode.SQL_ROLLBACK))) {
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
                } // SYNCHRONIZED

                if (changeDAO) {
                    try {
                        boolean autoCommit = dao.getConnection().getAutoCommit();
                        dao = getDatabaseProxy(databaseName);
                        //OutputMessage.println("[" + proxyId + "]: Trying to get a dao...");
                        while (dao == null) {
                          dao = getDatabaseProxy(databaseName);  
                          try {
                             Thread.sleep(1000);
                           } catch (InterruptedException ex) {
                             Logger.getLogger(QoSDBCConnectionProxy.class.getName()).log(Level.SEVERE, null, ex);
                           }
                        }
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

                Object message = inputStream.readObject();
                synchronized (this) { // SYNCHRONIZED
                    if (message instanceof Request) {
                        Request msg = (Request) message;
                        Response response = new Response();

                        long startTime = System.currentTimeMillis();

                        //OutputMessage.println("[" + proxyId + "]: " + "CODE: " + msg.getCode() 
                        //        + " COMMAND: " + msg.getCommand() + " DATABASE: " +  msg.getDatabase());
                        switch (msg.getCode()) {
                            case RequestCode.SQL_CONNECTION_CREATE: {
                                if (dao != null && dao.isActive()) {
                                    dao.close();
                                }
                                dao = getDatabaseProxy(msg.getDatabase());
                                //OutputMessage.println("[" + proxyId + "]: Trying to get a dao...");
                                while (dao == null) {
                                  dao = getDatabaseProxy(databaseName);  
                                    try {
                                        Thread.sleep(1000);
                                    } catch (InterruptedException ex) {
                                        Logger.getLogger(QoSDBCConnectionProxy.class.getName()).log(Level.SEVERE, null, ex);
                                    }
                                }
                                //OutputMessage.println("[" + proxyId + "]: GOT IT");
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
                                changeDAO = true;
                                try {
                                    Statement statement = dao.getConnection().createStatement(); // AO MUDAR O DAO, ESTAMOS PERDENDO OS STATEMENTS
                                    statement.setEscapeProcessing(false);
                                    statementList.put(Long.parseLong(msg.getParameterValue("statementId").toString()), statement);
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
                                    getStatement(Long.parseLong(msg.getParameterValue("statementId").toString())).close();
                                    statementList.remove(Long.parseLong(msg.getParameterValue("statementId").toString()));
                                    //OutputMessage.println("[" + proxyId + "]: " + "SUCCESS: " + "SQL_STATEMENT_CLOSE");
                                } catch (SQLException ex) {
                                    OutputMessage.println("[" + proxyId + "]: " + "FAILURE: " + "SQL_STATEMENT_CLOSE");
                                    response.setState(RequestCode.STATE_FAILURE);
                                }
                                break;
                            }
                            case RequestCode.SQL_RESULTSET_CREATE: {
                                try {
                                    Statement statement = getStatement(Long.parseLong(msg.getParameterValue("statementId").toString()));
                                    ResultSet resultSet = null;
                                    resultSet = statement.executeQuery(msg.getCommand());
                                    List<Row> resultSetList = transformResultSetToRowList(resultSet);
                                    response.setResultObject(resultSetList);
                                    this.resultSetList.put(Long.parseLong(msg.getParameterValue("resultSetId").toString()), resultSet);
                                    response.setState(RequestCode.STATE_SUCCESS);
                                    response.setAffectedRows(resultSetList.size());
                                    if (resultSetList.isEmpty()) {
                                        OutputMessage.println("[" + proxyId + "]: " + "FAILURE: SQL_RESULTSET_CREATE << " + msg.getCommand());
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
                                    getResultSet(Long.parseLong(msg.getParameterValue("resultSetId").toString())).close();
                                    resultSetList.remove(Long.parseLong(msg.getParameterValue("resultSetId").toString()));
                                } catch (SQLException ex) {
                                    OutputMessage.println("[" + proxyId + "]: " + "FAILURE: " + "SQL_RESULTSET_CLOSE");
                                    response.setState(RequestCode.STATE_FAILURE);
                                }
                                break;
                            }
                            case RequestCode.SQL_COMMIT: {
                                msg.setCommand("COMMIT");
                                dao.commit();
                                changeDAO = true;
                                response.setState(RequestCode.STATE_SUCCESS);
                                dao.close();
                                break;
                            }
                            case RequestCode.SQL_ROLLBACK: {
                                msg.setCommand("ROLLBACK");
                                dao.rollback();
                                changeDAO = true;
                                response.setState(RequestCode.STATE_SUCCESS);
                                break;
                            }
                            case RequestCode.SQL_UPDATE: {
                                int result;
                                result = dao.update(msg.getCommand(), getStatement(Long.parseLong(msg.getParameterValue("statementId").toString())));
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
                                    boolean newValue = Boolean.parseBoolean(msg.getParameterValue("autoCommit").toString());
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

                        long finishTime = System.currentTimeMillis();
                        response.setStartTime(startTime);
                        response.setFinishTime(finishTime);

                        if (msg.getCommand() != null && (msg.getCode() == RequestCode.SQL_UPDATE || msg.getCode() == RequestCode.SQL_RESULTSET_CREATE || msg.getCode() == RequestCode.SQL_COMMIT || msg.getCode() == RequestCode.SQL_ROLLBACK)) {
                            if (msg.getCode() == RequestCode.SQL_ROLLBACK) {
                                try {
                                    Statement statement = logConnection.createStatement();
                                    int result = statement.executeUpdate("DELETE FROM sql_log WHERE transaction_id = " + msg.getTransactionId());
                                    OutputMessage.println("[" + proxyId + "]: # OF ROWS DELETED: " + result);
                                    statement.close();
                                } catch (SQLException ex) {
                                    
                                }
                            } else {
                                log(msg.getCommand(), dao.getVmId(), dao.getDbName(), msg.getCode(), (finishTime - startTime), msg.getSlaResponseTime(), msg.getConnectionId(), msg.getTransactionId(), response.getAffectedRows(), flagMigration);
                            }
                        }

                        outputStream.writeObject(response);
                        outputStream.reset();

                        lastRequest = msg;

                        if (closeConnection) {
                            OutputMessage.println("[" + proxyId + "]: Closing proxy connection");
                            if (dbConnection != null) {
                                try {
                                    dbConnection.close();
                                } catch (IOException ex1) {
                                    dbConnection = null;
                                }
                            }
                            break;
                        }
                    }
                } // SYNCHRONIZED
            } catch (IOException ex) {
                OutputMessage.println("[" + proxyId + "]: Closing proxy connection");
                if (dbConnection != null) {
                    try {
                        dbConnection.close();
                    } catch (IOException ex1) {
                        dbConnection = null;
                    }
                }
                break;
            } catch (ClassNotFoundException ex) {
                OutputMessage.println("[" + proxyId + "]: Closing proxy connection");
                if (dbConnection != null) {
                    try {
                        dbConnection.close();
                    } catch (IOException ex1) {
                        dbConnection = null;
                    }
                }
                break;
            } // TRY
        } // WHILE
        // Close all database connections
        dao.close();
        qosdbcService.removeConnectionProxy(this);
        OutputMessage.println("[" + proxyId + "]: Proxy connection ended");
        //pw.close();
    }

    /**
     *
     * @param resultSet
     * @return
     */
    private static List<Row> transformResultSetToRowList(ResultSet resultSet) {
        List<Row> resultSetList = new ArrayList<Row>();
        try {
            java.sql.ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                Row row = new Row();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    Column column = new Column();
                    column.setColumnIndex(i);
                    column.setColumnLabel(metaData.getColumnLabel(i));
                    column.setColumnValue(resultSet.getObject(metaData.getColumnLabel(i)));
                    row.addColumn(column);
                }
                resultSetList.add(row);
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
}