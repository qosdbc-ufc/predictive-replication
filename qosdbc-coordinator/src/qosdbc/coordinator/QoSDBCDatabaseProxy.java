/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

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

    public QoSDBCDatabaseProxy(String dbDriver, String dbURL, String dbName, String dbUser, String dbPassword, String vmId) {
        this.dbName = dbName;
        this.vmId = vmId;
        try {
            Class.forName(dbDriver);
            connection = DriverManager.getConnection(dbURL, dbUser, dbPassword);
            connection.setAutoCommit(true);
            connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
        } catch (SQLException ex) {
            connection = null;
        } catch (ClassNotFoundException ex) {
            connection = null;
        }
    }

    public synchronized boolean isActive() {
        try {
            return (connection != null && !connection.isClosed());
        } catch (SQLException ex) {
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