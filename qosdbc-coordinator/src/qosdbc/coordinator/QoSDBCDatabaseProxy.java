/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import qosdbc.commons.data_structure.ReplicasList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import qosdbc.commons.OutputMessage;

/**
 *
 * @author Leonardo Oliveira Moreira
 *
 * Class that abstracts and maintains a JDBC connection with a database
 */
public class QoSDBCDatabaseProxy {

    private Connection connection;
    private String dbName;
    private String vmId;
    private long id;

    public QoSDBCDatabaseProxy(String dbDriver, String dbURL, String dbName, String dbUser, String dbPassword, String vmId, boolean autoCommit) {
        //OutputMessage.println("[QoSDBCDatabaseProxy]: " + dbDriver + " " + dbURL + " " + dbName + " " + dbUser + " " + dbPassword + " " + vmId);
        this.dbName = dbName;
        this.vmId = vmId;
        this.id = System.currentTimeMillis();
        try {
            Class.forName(dbDriver);
            connection = DriverManager.getConnection(dbURL, dbUser, dbPassword);
            connection.setAutoCommit(autoCommit);
            connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
        } catch (SQLException ex) {
            OutputMessage.println("ERROR - QoSDBCDatabaseProxy - SQLException");
            ex.printStackTrace();
            connection = null;
        } catch (ClassNotFoundException ex) {
            OutputMessage.println("ERROR - QoSDBCDatabaseProxy - ClassNotFoundException");
            ex.printStackTrace();
            connection = null;
        }
    }
    
    public long getId() {
        return id;
    }

    public synchronized boolean isActive() {
        try {
            return (connection != null && !connection.isClosed());
        } catch (SQLException ex) {
            OutputMessage.println("ERROR - QoSDBCDatabaseProxy - IsActive Exception");
            return false;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            connection = null;
        }
        super.finalize();
    }

    public synchronized int update(String sql, Statement statement) {
        try {
            int result = statement.executeUpdate(sql);
            return result;
        } catch (SQLException ex) {
            OutputMessage.println("[QoSDBCDatabaseProxy] ERROR ON UPDATE\n" + ex.getMessage());
            return -1;
        }
        // @gabiarra
        //return 0;
    }

    public synchronized void close() {
        try {
            connection.close();
        } catch (SQLException ex) {
        }
    }

    public synchronized void commit() {
        try {
            connection.commit();
        } catch (SQLException ex) {
        }
    }

    public synchronized void rollback() {
        try {
            connection.rollback();
        } catch (SQLException ex) {
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getVmId() {
        return vmId;
    }

    public void setVmId(String vmId) {
        this.vmId = vmId;
    }
    
}