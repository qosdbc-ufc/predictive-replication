/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.agent;

import qosdbc.commons.ShellCommand;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import qosdbc.commons.Database;
import qosdbc.commons.DatabaseSystem;
import qosdbc.commons.OutputMessage;

/**
 *
 * @author Leonardo Oliveira Moreira
 *
 * Agent that monitors and sends the monitorated data to the services qosdbc:
 * catalog and log.
 *
 * DBMSs Supported: - MySQL (Total) - PostgreSQL (Partial)
 */
public class QoSDBCMonitor extends Thread {

    private String vmId;
    private long time;
    private List<DatabaseSystem> databaseSystems;
    private Connection catalogConnection;

    /**
     * Construtor of the Agent Notifier
     *
     * @param vmId - Virtual Machine Identification
     * @param time - Time used to establish a delay
     * @param catalogConnection - 
     * @param databaseSystems - List of DBMSs that exists in this Virtual Machine
     * @throws SQLException 
     */
    public QoSDBCMonitor(String vmId, long time, Connection catalogConnection, List<DatabaseSystem> databaseSystems) throws SQLException {
        this.vmId = vmId;
        this.time = time;
        this.catalogConnection = catalogConnection;
        this.databaseSystems = databaseSystems;
    }

    /**
     * Lifecycle agent
     */
    @Override
    public void run() {
        qosdbc.commons.OutputMessage.println("QoSDBCMonitor: " + "is running");
        while (catalogConnection != null) {
            // Delay - Begin
            long currentTime = System.currentTimeMillis();
            long nextTime = currentTime + time;
            while (currentTime <= nextTime) {
                currentTime = System.currentTimeMillis();
            }
            // Delay - End
            // vm_state - Begin
            try {
                Statement statement = catalogConnection.createStatement();
                long memoryTotal = ShellCommand.getMemoryTotal(); 
                long diskTotal = ShellCommand.getDiskTotal();
                double cpuFree = ShellCommand.getCPUFreePercentage();
                long memoryFree = ShellCommand.getMemoryFree();
                long diskFree = ShellCommand.getDiskFree();
                int result = statement.executeUpdate(getVMStateInsertSQL(memoryTotal, diskTotal, cpuFree, memoryFree, diskFree));
                OutputMessage.println("VMID: " + vmId + " CPUF: " + cpuFree + " MEMT: " + memoryTotal + " MEMF: " + memoryFree + " DIST: " + diskTotal + " DISF " + diskFree + "");
                statement.close();
            } catch (SQLException ex) {
            }
            // vm_state - End
            // db_state - Begin
            for (DatabaseSystem databaseSystem : databaseSystems) {
                switch (databaseSystem.getType()) {
                    case qosdbc.commons.DatabaseSystem.TYPE_MYSQL: {
                        List<Database> mysqlDatabases = ShellCommand.getDatabases(databaseSystem.getType(), databaseSystem.getUser(), databaseSystem.getPassword());
                        try {
                            Statement statement = catalogConnection.createStatement();
                            for (Database database : mysqlDatabases) {
                                String dbName = database.getName();
                                long dbSize = getDatabaseSize(catalogConnection, dbName, database.getType());
                                long dbmsConnections = getDBMSActiveConnections(catalogConnection, database.getType());
                                int result = statement.executeUpdate(getDBStateInsertSQL(dbName, dbSize, dbmsConnections, database.getType()));
                                OutputMessage.println("VMID: " + vmId + " DBNA: " + dbName + " DBSI: " + dbSize + " NCON: " + dbmsConnections + " DBTY: " + database.getType());
                            }
                            statement.close();
                        } catch (SQLException ex) {
                        }
                        break;
                    }
                    case qosdbc.commons.DatabaseSystem.TYPE_POSTGRES: {
                        List<Database> postgresDatabases = ShellCommand.getDatabases(databaseSystem.getType(), databaseSystem.getUser(), databaseSystem.getPassword());
                        try {
                            Statement statement = catalogConnection.createStatement();
                            for (Database database : postgresDatabases) {
                                String dbName = database.getName();
                                long dbSize = getDatabaseSize(catalogConnection, dbName, database.getType());
                                long dbmsConnections = getDBMSActiveConnections(catalogConnection, database.getType());
                                int result = statement.executeUpdate(getDBStateInsertSQL(dbName, dbSize, dbmsConnections, database.getType()));
                                OutputMessage.println("VMID: " + vmId + " DBNA: " + dbName + " DBSI: " + dbSize + " NCON: " + dbmsConnections + " DBTY: " + database.getType());
                            }
                            statement.close();
                        } catch (SQLException ex) {
                        }
                        break;
                    }
                }
            }
            // db_state - End
        }
        try {
            catalogConnection.close();
        } catch (SQLException ex) {
            catalogConnection = null;
        }
    }

    /**
     * Method that creates the SQL to insert a entry in Catalog (table db_state)
     *
     * @param dbName - Database Name
     * @param dbSize - Database Size
     * @param dbmsConnections - DBMS Connections Number
     * @param dbmsType - DBMS's Type
     * @return - The SQL query to insert a entry in Catalog
     */
    private String getDBStateInsertSQL(String dbName, long dbSize, long dbmsConnections, int dbmsType) {
        String sql = "";
        sql += "INSERT INTO db_state (\"time\", vm_id, db_name, db_size, dbms_connections, dbms_type) VALUES ";
        sql += "(now(), '" + vmId + "', '" + dbName + "', " + dbSize + ", " + dbmsConnections + ", " + dbmsType + ")";
        return sql;
    }
    
    /**
     * Method that creates the SQL to insert a entry in Catalog (table vm_state)
     * 
     * @param memoryTotal
     * @param diskTotal
     * @param cpuFree
     * @param memoryFree
     * @param diskFree
     * @return - The SQL query to insert a entry in Catalog
     */
    private String getVMStateInsertSQL(long memoryTotal, long diskTotal, double cpuFree, long memoryFree, long diskFree) {
        String sql = "";
        sql += "INSERT INTO vm_state (\"time\", vm_id, mem_total, disk_total, cpu_free, mem_free, disk_free) VALUES ";
        sql += "(now(), '" + vmId + "', " + memoryTotal + ", " + diskTotal + ", " + cpuFree + ", " + memoryFree + ", " + diskFree + ")";
        return sql;
    }

    /**
     * Method that creates the SQL to verify the database size in megabytes
     *
     * @param dbName - Database Name
     * @param dbmsType - DBMS's Type
     * @return - The SQL query to verify the database size in megabytes
     */
    private String getDatabaseSizeSQL(String dbName, int dbmsType) {
        switch (dbmsType) {
            case qosdbc.commons.DatabaseSystem.TYPE_MYSQL: {
                String sql = "SELECT a.db_name, a.db_size ";
                sql += "FROM (SELECT table_schema AS db_name, (sum(data_length + index_length)/1024/1024) AS db_size " + 
                       "FROM information_schema.TABLES GROUP BY table_schema = '" + dbName + "') a WHERE a.db_name = '" + dbName + "'";
                return sql;
            }
            case qosdbc.commons.DatabaseSystem.TYPE_POSTGRES: {
                String sql = "SELECT (pg_database_size('" + dbName + "')/1024/1024) as db_size";
                return sql;
            }
        }
        return null;
    }

    /**
     * Method to get the active connections number in the DBMS
     *
     * @param connection - Connection with the DBMS monitored
     * @param dbmsType - DBMS's Type
     * @return - Number of active connections with the DBMS
     */
    private long getDBMSActiveConnections(Connection connection, int dbmsType) {
        long result = 0;
        switch (dbmsType) {
            case qosdbc.commons.DatabaseSystem.TYPE_MYSQL: {
                try {
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("show status like '%threads_connected%'");
                    while (resultSet != null && resultSet.next()) {
                        result = resultSet.getLong(2);
                    }
                    resultSet.close();
                    statement.close();
                } catch (SQLException ex) {
                }
                break;
            }
            case qosdbc.commons.DatabaseSystem.TYPE_POSTGRES: {
                try {
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("select count(*) as connections from pg_stat_activity");
                    while (resultSet != null && resultSet.next()) {
                        result = resultSet.getLong("connections");
                    }
                    resultSet.close();
                    statement.close();
                } catch (SQLException ex) {
                }
                break;
            }
        }
        return result;
    }

    /**
     * Method used to retrieve the database size in megabytes
     *
     * @param connection - Connection with the DBMS monitored
     * @param dbName - Database Name
     * @param dbmsType - DBMS's Type
     * @return - Database Size in megabytes
     */
    private long getDatabaseSize(Connection connection, String dbName, int dbmsType) {
        long result = 0;
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(getDatabaseSizeSQL(dbName, dbmsType));
            while (resultSet != null && resultSet.next()) {
                result = resultSet.getLong("db_size");
            }
            resultSet.close();
            statement.close();
        } catch (SQLException ex) {
        }
        return result;
    }
}