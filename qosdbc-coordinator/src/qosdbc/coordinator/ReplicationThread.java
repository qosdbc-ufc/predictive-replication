/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.io.BufferedOutputStream;
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
import java.util.concurrent.TimeUnit;
import java.io.PrintWriter;
import java.io.FileWriter;

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
    private QoSDBCLoadBalancer loadBalancer;

    public ReplicationThread(Command command, 
            Connection catalogConnection, 
            Connection logConnection, 
            QoSDBCService qosdbcService , QoSDBCLoadBalancer loadBalancer) {
        this.command = command;
        this.catalogConnection = catalogConnection;
        this.logConnection = logConnection;
        this.qosdbcService = qosdbcService;
        this.loadBalancer = loadBalancer;
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

        this.qosdbcService.setDbReplicationStatus(databaseName, true);

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
            long startTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            long timestamp = 0;

            /* Socket to connect destination agent */
            Socket socketDestinationAgent = new Socket(destinationHost, destinationAgentPort);
            ObjectOutputStream outputStreamDestinationAgent = new ObjectOutputStream(new BufferedOutputStream(socketDestinationAgent.getOutputStream()));
            ObjectInputStream inputStreamDestinationAgent = new ObjectInputStream((socketDestinationAgent.getInputStream()));
            /* Socket to connect source agent */
            Socket socketSourceAgent = new Socket(sourceHost, sourceAgentPort);
            ObjectOutputStream outputStreamSourceAgent = new ObjectOutputStream(new BufferedOutputStream(socketSourceAgent.getOutputStream()));
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

            timestamp = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            outputStreamDestinationAgent.writeObject(commandCreate);
            outputStreamDestinationAgent.flush();

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
                    + " " + ((TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - timestamp) / 1000) + " secs");
            /* Create database into destination agent - End */

            /* Pause all connection proxies of the database - Begin */
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: PAUSE (BEFORE DUMP) " + "START");
            long logStartTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            qosdbcService.setInMigration(databaseName, true);
            qosdbcService.pauseDatabaseConnections(databaseName);
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: PAUSE (BEFORE DUMP) " + "END");
            /* Pause all connection proxies of the database - End */

            /* Generate dump database file in source agent - Begin */
            this.qosdbcService.finishAllLoggingByNow(databaseName);
            Thread t = this.qosdbcService.flushTempLogBlocking(databaseName);
            t.start();
            t.join();
            OutputMessage.println("[" + "ReplicationThread] DONE PENDING LOG FLUSH");
            Command commandDump = new Command();
            commandDump.setCode(CommandCode.DATABASE_DUMP);
            commandDump.setParameters(hashMap);
            commandDump.toString();

            timestamp = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            outputStreamSourceAgent.writeObject(commandDump);
            outputStreamSourceAgent.flush();

            Object objectDump = inputStreamSourceAgent.readObject();
            Return resultDump = (Return) objectDump;

            String dumpFileURL = null;
            switch (resultDump.getState()) {
                case CommandCode.STATE_SUCCESS: {
                    dumpFileURL = (String) resultDump.getResultObject();
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: DATABASE_DUMP: " + dumpFileURL);
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
                    + " " + ((TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - timestamp) / 1000) + " secs");
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
            HashMap<String, Object> hashMapRe = new HashMap<String, Object>();
            hashMapRe.put("databaseName", databaseName);
            hashMapRe.put("username", "root");
            hashMapRe.put("password", "ufc123");
            hashMapRe.put("databaseType", databaseSystem);
            hashMapRe.put("dumpFileUrl", dumpFileURL);
            commandRestore.setParameters(hashMapRe);

            timestamp = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            outputStreamDestinationAgent.writeObject(commandRestore);
            outputStreamDestinationAgent.flush();

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
                    + ((TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - timestamp) / 1000) + " secs");
            /* Restore database in destination agent - Begin */



            /* Pause all connection proxies of the database - Begin */
            OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                    + "]: PAUSE (BEFORE UPDATES INFORMATION AND PROPAGATES UPDATE QUERIES) " + "START");
            qosdbcService.pauseDatabaseConnections(databaseName);
            OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                    + "]: PAUSE (BEFORE UPDATES INFORMATION AND PROPAGATES UPDATE QUERIES) " + "END");
            /* Pause all connection proxies of the database - End */

            timestamp = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            try {

                Statement logStatement = logConnection.createStatement();
                ResultSet logResultSet = logStatement.executeQuery(
                        "SELECT sql "
                        + "FROM sql_log "
                        + "WHERE time_local >= " + logStartTime + " "
                        + "AND (sql_type = " + RequestCode.SQL_UPDATE + " OR sql_type = "
                        + RequestCode.SQL_COMMIT + " OR sql_type = " + RequestCode.SQL_ROLLBACK + ") "
                        + "AND db_name = '" + databaseName + "' ORDER BY time_local ASC");

                Connection databaseConnection = DriverManager.getConnection("jdbc:mysql://"
                                + destinationHost + ":" + 3306 + "/" + databaseName, "root", "ufc123");
                Statement databaseStatement = databaseConnection.createStatement();

                int count = 0;
                //PrintWriter pw = new PrintWriter(new FileWriter("/var/www/html/qosdbc/sync.sql"));
                while (logResultSet.next()) {
                    String sql = logResultSet.getString("sql");
                    //pw.println(sql+";");
                    databaseStatement.addBatch(sql);
                    if(count % 1000 == 0) {
                        try {
                            databaseStatement.executeBatch();
                            databaseStatement.clearBatch();
                        } catch (SQLException e) {

                        }
                    }
                    count++;
                }
                //pw.close();
                //OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Done writing sync file\n");
                try {
                    databaseStatement.executeBatch();
                } catch (SQLException e) {

                }
                databaseStatement.close();
                databaseConnection.close();

                logResultSet.close();
                logStatement.close();


                /*
                Command syncCommanc = new Command();
                syncCommanc.setCode(CommandCode.DATABASE_SYNC);
                HashMap<String, Object> hashMapSync = new HashMap<String, Object>();
                hashMapSync.put("databaseName", databaseName);
                hashMapSync.put("username", "root");
                hashMapSync.put("password", "ufc123");
                hashMapSync.put("databaseType", databaseSystem);
                hashMapSync.put("syncFileUrl", "http://172.31.37.249/qosdbc/sync.sql");
                syncCommanc.setParameters(hashMapSync);

                outputStreamDestinationAgent.writeObject(syncCommanc);
                outputStreamDestinationAgent.flush();

                Object objectSync = inputStreamDestinationAgent.readObject();
                Return resultSync = (Return) objectSync;
                */
                OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Propagate Log Update Query Success\n");
                OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                        + "]: Propagate Log Update Query " + "[OK]" + " "
                        + ((TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - timestamp) / 1000) + " secs" + " (" + count + ") records propagated");
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
                OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Propagate Log Update Query Failure\n");
                OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                        + "]: Propagate Log Update Query " + "[FAILURE]" + " "
                        + ((TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - timestamp) / 1000) + " secs");
            } /*catch (IOException ex) {
                System.out.println(ex.getMessage());
                OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Propagate Log Update Query Failure\n");
                OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                        + "]: Propagate Log Update Query " + "[FAILURE]" + " "
                        + ((TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - timestamp) / 1000) + " secs");
            }*/


            /* Play all paused connection proxies of the database - Begin */
            OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                    + "]: PLAY (AFTER UPDATES INFORMATION AND PROPAGATES UPDATE QUERIES) " + "START");
            qosdbcService.setInMigration(databaseName, false);
            qosdbcService.playDatabaseConnections(databaseName);
            OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                    + "]: PLAY (AFTER UPDATES INFORMATION AND PROPAGATES UPDATE QUERIES) " + "END");
            /* Play all paused connection proxies of the database - End */

            
            /* Update database information in db_active and db_state - Begin */
            timestamp = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            try {

                // Replication option
                String sqlDbActiveReplica = "INSERT INTO db_active_replica (\"time\", vm_id, master) VALUES (now(), '"
                        + destinationHost + "', '" + sourceHost + "/" + databaseName + "')";
                OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: " + sqlDbActiveReplica);
                Statement statement = catalogConnection.createStatement();
                int dbActiveReplica = statement.executeUpdate(sqlDbActiveReplica);
                statement.close();

                if (dbActiveReplica > 0) {
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Catalog Information update - Success!\n");
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                            + "]: Database replica insertion into db_active_replica " + "[OK]" + " "
                            + ((TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - timestamp) / 1000) + " secs");
                } else {
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Catalog Information update - Failure\n");
                    OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                            + "]: Database replica insertion into db_active_replica " + "[FAILURE]"
                            + " " + ((TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - timestamp) / 1000) + " secs");
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
                OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: Update Catalog Information Failure\n");
                OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                        + "]: Database replica insert into db_active_replica " + "[FAILURE]"
                        + " " + ((TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - timestamp) / 1000) + " secs");
            }
            /* Update database information in db_active and db_state - End */
            
            
            socketDestinationAgent.close();
            socketSourceAgent.close();
            long endTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                    + "]: REPLICATION TOTAL TIME " + ((endTime - startTime) / 1000) + " secs");

            this.loadBalancer.addReplica(databaseName, destinationHost);

            this.qosdbcService.setDbReplicationStatus(databaseName, false);
            OutputMessage.println("[" + "ReplicationThread_" + this.getId()
                    + "]: FINISHED REPLICATION FOR GOOD!");


        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            OutputMessage.println("[" + "ReplicationThread_" + this.getId() + "]: ERROR(2)\n");
            ex.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
