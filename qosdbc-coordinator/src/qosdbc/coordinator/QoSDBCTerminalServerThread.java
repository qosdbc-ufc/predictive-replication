/*
 * To change this template, choose Tools | Templates
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
 * @author Leonardo Oliveira Moreira
 */
public class QoSDBCTerminalServerThread extends Thread {

    private Socket socket;
    private Connection catalogConnection;
    private Connection logConnection;
    private QoSDBCService qosdbcService;

    public QoSDBCTerminalServerThread(Socket socket, Connection catalogConnection, Connection logConnection, QoSDBCService qosdbcService) {
        this.socket = socket;
        this.catalogConnection = catalogConnection;
        this.logConnection = logConnection;
        this.qosdbcService = qosdbcService;
    }

    @Override
    public void run() {
        OutputMessage.println("[" + this.getId() + "]: Terminal connection starting");
        ObjectOutputStream outputStream = null;
        ObjectInputStream inputStream = null;
        boolean proceed = true;
        try {
            outputStream = new ObjectOutputStream((socket.getOutputStream()));
            inputStream = new ObjectInputStream((socket.getInputStream()));
        } catch (IOException ex) {
            OutputMessage.println("[" + this.getId() + "]: Closing terminal connection");
            proceed = false;
        }
        OutputMessage.println("[" + this.getId() + "]: Terminal connection started");

        while (proceed && socket != null && socket.isConnected()) {
            try {               
                Object message = inputStream.readObject();
                Command command = (Command) message;
                Return result = new Return();
                result.setState(CommandCode.STATE_FAILURE);
                switch (command.getCode()) {
                    case CommandCode.DATABASE_CREATE: {
                        /* TO DO */
                        result.setState(CommandCode.STATE_SUCCESS);
                        break;
                    }
                    case CommandCode.DATABASE_DROP: {
                        /* TO DO */
                        result.setState(CommandCode.STATE_SUCCESS);
                        break;
                    }
                    case CommandCode.DATABASE_RESTORE: {
                        /* TO DO */
                        result.setState(CommandCode.STATE_SUCCESS);
                        break;
                    }
                    case CommandCode.TERMINAL_MIGRATE: {
                        String sourceHost = (String) command.getParameterValue("sourceHost");
                        String databaseName = (String) command.getParameterValue("databaseName");
                        String databaseSystem = (String) command.getParameterValue("databaseSystem");
                        String destinationHost = (String) command.getParameterValue("destinationHost");

                        int destinationAgentPort = -1;
                        int sourceAgentPort = -1;
                        try {
                            Statement statement = catalogConnection.createStatement();
                            ResultSet resultSetDestination = statement.executeQuery("SELECT vm_id, agent_port FROM vm_active WHERE vm_id = '" + destinationHost + "'");
                            while (resultSetDestination.next()) {
                                String agentPort = resultSetDestination.getString("agent_port");
                                if (agentPort != null && agentPort.trim().length() > 0) {
                                    destinationAgentPort = Integer.parseInt(agentPort);
                                }
                            }
                            resultSetDestination.close();
                            ResultSet resultSetSource = statement.executeQuery("SELECT vm_id, agent_port FROM vm_active WHERE vm_id = '" + sourceHost + "'");
                            while (resultSetSource.next()) {
                                String agentPort = resultSetSource.getString("agent_port");
                                if (agentPort != null && agentPort.trim().length() > 0) {
                                    sourceAgentPort = Integer.parseInt(agentPort);
                                }
                            }
                            resultSetSource.close();
                            statement.close();
                        } catch (SQLException ex) {
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

                            result.setResultObject("");

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
                                    result.setResultObject(result.getResultObject().toString() + "Create Success\n");
                                    break;
                                }
                                case CommandCode.STATE_FAILURE: {
                                    result.setResultObject(result.getResultObject().toString() + "Create Failure\n");
                                    break;
                                }
                            }
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: Create Database " + (resultCreate.getState() == CommandCode.STATE_SUCCESS ? "[OK]" : "[FAILURE]") + " " + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
                            /* Create database into destination agent - End */

                            /* Pause all connection proxies of the database - Begin */
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: PAUSE (BEFORE DUMP) " + "START");
                            long logStartTime = System.currentTimeMillis();
                            qosdbcService.setInMigration(databaseName, true);
                            qosdbcService.pauseDatabaseConnections(databaseName);
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: PAUSE (BEFORE DUMP) " + "END");
                            /* Pause all connection proxies of the database - End */

                            /* Generate dump database file in source agent - Begin */
                            Command commandDump = new Command();
                            commandDump.setCode(CommandCode.DATABASE_DUMP);
                            commandDump.setParameters(hashMap);

                            timestamp = System.currentTimeMillis();
                            outputStreamSourceAgent.writeObject(commandDump);
                            outputStreamSourceAgent.reset();

                            Object objectDump = inputStreamSourceAgent.readObject();
                            Return resultDump = (Return) objectDump;

                            String dumpFileURL = null;
                            switch (resultDump.getState()) {
                                case CommandCode.STATE_SUCCESS: {
                                    dumpFileURL = (String) resultDump.getResultObject();
                                    result.setResultObject(result.getResultObject().toString() + "Dump Success\n");
                                    break;
                                }
                                case CommandCode.STATE_FAILURE: {
                                    result.setResultObject(result.getResultObject().toString() + "Dump Failure\n");
                                    break;
                                }
                            }
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: Dump Database " + (resultDump.getState() == CommandCode.STATE_SUCCESS ? "[OK]" : "[FAILURE]") + " " + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
                            /* Generate dump database file in source agent - End */

                            /* Play all paused connection proxies of the database - Begin */
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: PLAY (AFTER DUMP) " + "START");
                            qosdbcService.playDatabaseConnections(databaseName);
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: PLAY (AFTER DUMP) " + "END");
                            /* Play all paused connection proxies of the database - End */

                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: Restoring...");
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
                                    result.setResultObject(result.getResultObject().toString() + "Restore Success\n");
                                    break;
                                }
                                case CommandCode.STATE_FAILURE: {
                                    result.setResultObject(result.getResultObject().toString() + "Restore Failure\n");
                                    break;
                                }
                            }
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: Restore Database " + (resultRestore.getState() == CommandCode.STATE_SUCCESS ? "[OK]" : "[FAILURE]") + " " + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
                            /* Restore database in destination agent - Begin */

                            /* Update database information in db_active and db_state - Begin */
                            timestamp = System.currentTimeMillis();
                            try {
                                // Migrating option
                                //String sqlDbActive = "UPDATE db_active SET vm_id = '" + destinationHost + "' WHERE vm_id = '" + sourceHost + "' AND db_name = '" + databaseName + "'";
                                //String sqlDbState = "UPDATE db_state SET vm_id = '" + destinationHost + "' WHERE vm_id = '" + sourceHost + "' AND db_name = '" + databaseName + "'";
                                
                                // Replication option
                                String sqlDbActiveReplic = "INSERT INTO db_active_replica (\"time\", vm_id, master) VALUES (now(), '" + destinationHost + "', '" + sourceHost+ "/" +databaseName + "'";
                                
                                Statement statement = catalogConnection.createStatement();
                                int dbActiveReplic = statement.executeUpdate(sqlDbActiveReplic);
                                // int dbState = statement.executeUpdate(sqlDbState);
                                statement.close();
                                if (dbActiveReplic > 0) {
                                    result.setResultObject(result.getResultObject().toString() + "Update Catalog Information Success\n");
                                    OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: Database replic insert into db_active_replic " + "[OK]" + " " + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
                                } else {
                                    result.setResultObject(result.getResultObject().toString() + "Update Catalog Information Failure\n");
                                    OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: Database replic insert into db_active_replic " + "[FAILURE]" + " " + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
                                }
                            } catch (SQLException ex) {
                                System.out.println(ex.getMessage());
                                result.setResultObject(result.getResultObject().toString() + "Update Catalog Information Failure\n");
                                OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: Database replic insert into db_active_replic " + "[FAILURE]" + " " + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
                            }
                            /* Update database information in db_active and db_state - End */

                            /* Pause all connection proxies of the database - Begin */
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: PAUSE (BEFORE UPDATES INFORMATION AND PROPAGATES UPDATE QUERIES) " + "START");
                            qosdbcService.pauseDatabaseConnections(databaseName);
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: PAUSE (BEFORE UPDATES INFORMATION AND PROPAGATES UPDATE QUERIES) " + "END");
                            /* Pause all connection proxies of the database - End */

                            timestamp = System.currentTimeMillis();
                            try {
                                Connection databaseConnection = DriverManager.getConnection("jdbc:mysql://" + destinationHost + ":" + 3306 + "/" + databaseName, "root", "ufc123");
                                Statement logStatement = logConnection.createStatement();
                                ResultSet logResultSet = logStatement.executeQuery(
                                        "SELECT sql "
                                        + "FROM sql_log "
                                        + "WHERE time_local >= " + logStartTime + " "
                                        + "AND (sql_type = " + RequestCode.SQL_UPDATE + " OR sql_type = " + RequestCode.SQL_COMMIT + " OR sql_type = " + RequestCode.SQL_ROLLBACK + ") "
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
                                result.setResultObject(result.getResultObject().toString() + "Propagate Log Update Query Success\n");
                                OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: Propagate Log Update Query " + "[OK]" + " " + ((System.currentTimeMillis() - timestamp) / 1000) + " secs" + " (" + count + ") records propagated");
                            } catch (SQLException ex) {
                                System.out.println(ex.getMessage());
                                result.setResultObject(result.getResultObject().toString() + "Propagate Log Update Query Failure\n");
                                OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: Propagate Log Update Query " + "[FAILURE]" + " " + ((System.currentTimeMillis() - timestamp) / 1000) + " secs");
                            }

                            /* Change database connection in database proxy - Start */
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: CHANGE CONNECTION " + "START");
                            qosdbcService.changeDatabaseConnection(databaseName);
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: CHANGE CONNECTION " + "END");
                            /* Change database connection in database proxy - End */

                            /* Play all paused connection proxies of the database - Begin */
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: PLAY (AFTER UPDATES INFORMATION AND PROPAGATES UPDATE QUERIES) " + "START");
                            qosdbcService.setInMigration(databaseName, false);
                            qosdbcService.playDatabaseConnections(databaseName);
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: PLAY (AFTER UPDATES INFORMATION AND PROPAGATES UPDATE QUERIES) " + "END");
                            /* Play all paused connection proxies of the database - End */

                            result.setState(CommandCode.STATE_SUCCESS);

                            socketDestinationAgent.close();
                            socketSourceAgent.close();
                            
                            long endTime = System.currentTimeMillis();
                            OutputMessage.println("[" + "TerminalThread_" + this.getId() + "]: MIGRATION TOTAL TIME " + ((endTime - startTime) / 1000) + " secs");
                        } catch (IOException ex) {
                        }
                        break;
                    }
                    case CommandCode.TERMINAL_LIST_ACTIVE_DATABASE: {
                        String resultReturn = "";
                        try {
                            Statement statement = catalogConnection.createStatement();
                            ResultSet resultSet = statement.executeQuery("SELECT \"time\", vm_id, db_name, dbms_type, dbms_user, dbms_password, dbms_port from db_active");
                            while (resultSet.next()) {
                                String dbmsUser = resultSet.getString("dbms_user");
                                String dbmsPassword = resultSet.getString("dbms_password");
                                String dbmsPort = resultSet.getString("dbms_port");
                                String dbName = resultSet.getString("db_name");
                                int dbmsType = resultSet.getInt("dbms_type");
                                String vmId = resultSet.getString("vm_id");
                                resultReturn += vmId + "\t" + dbName + "\t" + dbmsType + "\t" + dbmsPort + "\t" + dbmsUser + "\t" + dbmsPassword + "\n";
                            }
                            resultSet.close();
                            statement.close();
                            result.setState(CommandCode.STATE_SUCCESS);
                        } catch (SQLException ex) {
                            resultReturn = ex.getMessage();
                        }
                        result.setResultObject(resultReturn);
                        break;
                    }
                    case CommandCode.TERMINAL_LIST_ACTIVE_VIRTUAL_MACHINE: {
                        String resultReturn = "";
                        try {
                            Statement statement = catalogConnection.createStatement();
                            ResultSet resultSet = statement.executeQuery("SELECT \"time\", vm_id, mem_total, disk_total, agent_port from vm_active");
                            while (resultSet.next()) {
                                String memTotal = resultSet.getString("mem_total");
                                String diskTotal = resultSet.getString("disk_total");
                                String vmId = resultSet.getString("vm_id");
                                String agentPort = resultSet.getString("agent_port");
                                resultReturn += vmId + "\t" + memTotal + "\t" + diskTotal + "\t" + agentPort + "\n";
                            }
                            resultSet.close();
                            statement.close();
                            result.setState(CommandCode.STATE_SUCCESS);
                        } catch (SQLException ex) {
                            resultReturn = ex.getMessage();
                        }
                        result.setResultObject(resultReturn);
                        
                        break;
                    }
                }
                outputStream.writeObject(result);
                outputStream.reset();
            } catch (ClassNotFoundException ex) {
                try {
                    socket.close();
                } catch (IOException ex1) {
                }
                proceed = false;
            } catch (IOException ex) {
                try {
                    socket.close();
                } catch (IOException ex1) {
                }
                proceed = false;
            }
        }
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException ex) {
        }
        OutputMessage.println("[" + this.getId() + "]: Terminal connection closed");
    }
}