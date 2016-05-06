/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import qosdbc.commons.OutputMessage;
import qosdbc.commons.command.Command;
import qosdbc.commons.command.CommandCode;
import qosdbc.commons.command.Return;
import qosdbc.commons.jdbc.RequestCode;

/**
 *
 * @author serafim
 */
public class ReplicationThread extends Thread {

    private Command command;
    private Connection catalogConnection;
    private Connection logConnection;
    private QoSDBCService qosdbcService;

    public ReplicationThread(Command command, 
            Connection catalogConnection, 
            Connection logConnection, 
            QoSDBCService qosdbcService) {
        this.command = command;
        this.catalogConnection = catalogConnection;
        this.logConnection = logConnection;
        this.qosdbcService = qosdbcService;
    }

    @Override
    public void run() {
        String sourceHost = (String) command.getParameterValue("sourceHost");
        String databaseName = (String) command.getParameterValue("databaseName");
        String databaseSystem = (String) command.getParameterValue("databaseSystem");
        String destinationHost = (String) command.getParameterValue("destinationHost");
        OutputMessage.println("Replication is going to start: \n" + 
        "SourceHost: " + sourceHost + "\n" +
        "DatabaseName: " + databaseName + "\n" +
        "DatabaseSystem: " + databaseSystem + "\n" +
        "DestinationHost: " + destinationHost + "\n");
        
        int destinationAgentPort = -1;
        int sourceAgentPort = -1;
        try {
            Statement statement = catalogConnection.createStatement();
            ResultSet resultSetDestination = statement.executeQuery(
                    "SELECT vm_id, agent_port FROM vm_active WHERE vm_id = '"
                    + destinationHost + "'");
            while (resultSetDestination.next()) {
                String agentPort = resultSetDestination.getString("agent_port");
                if (agentPort != null && agentPort.trim().length() > 0) {
                    destinationAgentPort = Integer.parseInt(agentPort);
                }
            }
            resultSetDestination.close();
            ResultSet resultSetSource = statement.executeQuery(
                    "SELECT vm_id, agent_port FROM vm_active WHERE vm_id = '"
                    + sourceHost + "'");
            while (resultSetSource.next()) {
                String agentPort = resultSetSource.getString("agent_port");
                if (agentPort != null && agentPort.trim().length() > 0) {
                    sourceAgentPort = Integer.parseInt(agentPort);
                }
            }
            resultSetSource.close();
            statement.close();
        } catch (SQLException ex) {
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: ERROR(1)\n");
            ex.printStackTrace();
        }
        try {
            long startTime = System.currentTimeMillis();
            long timestamp = 0;

            /* Socket to connect destination agent */
            Socket socketDestinationAgent = new Socket(destinationHost, destinationAgentPort);
            ObjectOutputStream outputStreamDestinationAgent = new ObjectOutputStream((socketDestinationAgent.getOutputStream()));
            ObjectInputStream inputStreamDestinationAgent = new ObjectInputStream((socketDestinationAgent.getInputStream()));
            /* Socket to connect source agent */
            Socket socketSourceAgent = new Socket(sourceHost, sourceAgentPort);
            ObjectOutputStream outputStreamSourceAgent = new ObjectOutputStream((socketSourceAgent.getOutputStream()));
            ObjectInputStream inputStreamSourceAgent = new ObjectInputStream((socketSourceAgent.getInputStream()));

            HashMap<String, Object> hashMap = new HashMap<String, Object>();
            hashMap.put("databaseName", databaseName);
            hashMap.put("username", "root");
            hashMap.put("password", "ufc123");
            hashMap.put("databaseType", databaseSystem);

            /* Create database into destination agent - Begin */
            Command commandCreate = new Command();
            commandCreate.setCode(CommandCode.DATABASE_CREATE);
            commandCreate.setParameters(hashMap);

            timestamp = System.currentTimeMillis();
            outputStreamDestinationAgent.writeObject(commandCreate);
            outputStreamDestinationAgent.reset();

            Object objectCreate = inputStreamDestinationAgent.readObject();
            Return resultCreate = (Return) objectCreate;

            switch (resultCreate.getState()) {
                case CommandCode.STATE_SUCCESS: {
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: DATABASE_CREATE Success");
                    break;
                }
                case CommandCode.STATE_FAILURE: {
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: DATABASE_CREATE Failure");
                    break;
                }
            }
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Create Database "
                    + (resultCreate.getState() == CommandCode.STATE_SUCCESS ? "[OK]" : "[FAILURE]")
                    + " " + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
            /* Create database into destination agent - End */

            /* Pause all connection proxies of the database - Begin */
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: PAUSE (BEFORE DUMP) " + "START");
            long logStartTime = System.currentTimeMillis();
            qosdbcService.setInMigration(databaseName, true);
            qosdbcService.pauseDatabaseConnections(databaseName);
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: PAUSE (BEFORE DUMP) " + "END");
            /* Pause all connection proxies of the database - End */

            /* Generate dump database file in source agent - Begin */
            Command commandDump = new Command();
            commandDump.setCode(CommandCode.DATABASE_DUMP);
            commandDump.setParameters(hashMap);
            commandDump.toString();

            timestamp = System.currentTimeMillis();
            outputStreamSourceAgent.writeObject(commandDump);
            outputStreamSourceAgent.reset();

            Object objectDump = inputStreamSourceAgent.readObject();
            Return resultDump = (Return) objectDump;

            String dumpFileURL = null;
            switch (resultDump.getState()) {
                case CommandCode.STATE_SUCCESS: {
                    dumpFileURL = (String) resultDump.getResultObject();
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: DATABASE_DUMP Success");
                    break;
                }
                case CommandCode.STATE_FAILURE: {
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: DATABASE_DUMP Failure");
                    break;
                }
            }
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Dump Database "
                    + (resultDump.getState() == CommandCode.STATE_SUCCESS ? "[OK]" : "[FAILURE]")
                    + " " + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
            /* Generate dump database file in source agent - End */

            /* Play all paused connection proxies of the database - Begin */
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: PLAY (AFTER DUMP) " + "START");
            qosdbcService.playDatabaseConnections(databaseName);
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: PLAY (AFTER DUMP) " + "END");
            /* Play all paused connection proxies of the database - End */

            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Restoring...");
            /* Restore database in destination agent - Begin */
            Command commandRestore = new Command();
            commandRestore.setCode(CommandCode.DATABASE_RESTORE);
            hashMap.put("dumpFileURL", dumpFileURL);
            commandRestore.setParameters(hashMap);

            timestamp = System.currentTimeMillis();
            outputStreamDestinationAgent.writeObject(commandRestore);
            outputStreamDestinationAgent.reset();

            Object objectRestore = inputStreamDestinationAgent.readObject();
            Return resultRestore = (Return) objectRestore;

            switch (resultRestore.getState()) {
                case CommandCode.STATE_SUCCESS: {
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: DATABASE_RESTORE Success");
                    break;
                }
                case CommandCode.STATE_FAILURE: {
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: DATABASE_RESTORE Success");
                    break;
                }
            }
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Restore Database "
                    + (resultRestore.getState() == CommandCode.STATE_SUCCESS ? "[OK]" : "[FAILURE]") + " "
                    + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
            /* Restore database in destination agent - Begin */



            /* Pause all connection proxies of the database - Begin */
            OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                    + "]: PAUSE (BEFORE UPDATES INFORMATION AND PROPAGATES UPDATE QUERIES) " + "START");
            qosdbcService.pauseDatabaseConnections(databaseName);
            OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                    + "]: PAUSE (BEFORE UPDATES INFORMATION AND PROPAGATES UPDATE QUERIES) " + "END");
            /* Pause all connection proxies of the database - End */

            timestamp = System.currentTimeMillis();
            try {
                Connection databaseConnection = DriverManager.getConnection("jdbc:mysql://"
                        + destinationHost + ":" + 3306 + "/" + databaseName, "root", "ufc123");
                Statement logStatement = logConnection.createStatement();
                ResultSet logResultSet = logStatement.executeQuery(
                        "SELECT sql "
                        + "FROM sql_log "
                        + "WHERE time_local >= " + logStartTime + " "
                        + "AND (sql_type = " + RequestCode.SQL_UPDATE + " OR sql_type = "
                        + RequestCode.SQL_COMMIT + " OR sql_type = " + RequestCode.SQL_ROLLBACK + ") "
                        + "AND db_name = '" + databaseName + "' ORDER BY time_local ASC");
                Statement databaseStatement = databaseConnection.createStatement();
                int count = 0;
                while (logResultSet.next()) {
                    String sql = logResultSet.getString("sql");
                    try {
                        databaseStatement.executeUpdate(sql);
                    } catch (SQLException ex) {
                    }
                    count++;
                }
                databaseStatement.close();
                logResultSet.close();
                logStatement.close();
                databaseConnection.close();
                OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Propagate Log Update Query Success\n");
                OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                        + "]: Propagate Log Update Query " + "[OK]" + " "
                        + ((System.currentTimeMillis() - timestamp) / 1000) + " secs" + " (" + count + ") records propagated");
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
                OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Propagate Log Update Query Failure\n");
                OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                        + "]: Propagate Log Update Query " + "[FAILURE]" + " "
                        + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
            }

            /* Change database connection in database proxy - Start */
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: CHANGE CONNECTION " + "START");
            qosdbcService.changeDatabaseConnection(databaseName);
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: CHANGE CONNECTION " + "END");
            /* Change database connection in database proxy - End */

            /* Play all paused connection proxies of the database - Begin */
            OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                    + "]: PLAY (AFTER UPDATES INFORMATION AND PROPAGATES UPDATE QUERIES) " + "START");
            qosdbcService.setInMigration(databaseName, false);
            qosdbcService.playDatabaseConnections(databaseName);
            OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                    + "]: PLAY (AFTER UPDATES INFORMATION AND PROPAGATES UPDATE QUERIES) " + "END");
            /* Play all paused connection proxies of the database - End */

            
            /* Update database information in db_active and db_state - Begin */
            timestamp = System.currentTimeMillis();
            try {
                // Migrating option
                //String sqlDbActive = "UPDATE db_active SET vm_id = '" + destinationHost + "' WHERE vm_id = '" + sourceHost + "' AND db_name = '" + databaseName + "'";
                //String sqlDbState = "UPDATE db_state SET vm_id = '" + destinationHost + "' WHERE vm_id = '" + sourceHost + "' AND db_name = '" + databaseName + "'";

                // Replication option
                String sqlDbActiveReplica = "INSERT INTO db_active_replica (\"time\", vm_id, master) VALUES (now(), '"
                        + destinationHost + "', '" + sourceHost + "/" + databaseName + "')";
                OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: " + sqlDbActiveReplica);
                Statement statement = catalogConnection.createStatement();
                int dbActiveReplica = statement.executeUpdate(sqlDbActiveReplica);
                // int dbState = statement.executeUpdate(sqlDbState);
                statement.close();
                if (dbActiveReplica > 0) {
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Catalog Information update - Success!\n");
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                            + "]: Database replica insertion into db_active_replica " + "[OK]" + " "
                            + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
                } else {
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Catalog Information update - Failure\n");
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                            + "]: Database replica insertion into db_active_replica " + "[FAILURE]"
                            + " " + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
                OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Update Catalog Information Failure\n");
                OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                        + "]: Database replica insert into db_active_replica " + "[FAILURE]"
                        + " " + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
            }
            /* Update database information in db_active and db_state - End */
            
            
            socketDestinationAgent.close();
            socketSourceAgent.close();
            long endTime = System.currentTimeMillis();
            OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                    + "]: MIGRATION TOTAL TIME " + ((endTime - startTime) / 1000) + " secs");
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: ERROR(2)\n");
            ex.printStackTrace();
        }
    }
}
