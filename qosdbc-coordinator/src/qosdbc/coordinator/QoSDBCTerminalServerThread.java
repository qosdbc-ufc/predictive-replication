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


                        Command command1 = new Command();
                        command1.setCode(CommandCode.TERMINAL_MIGRATE);
                        HashMap<String, Object> commandParams = new HashMap<String, Object>();
                        commandParams.put("sourceHost", sourceHost);
                        commandParams.put("databaseName", databaseName);
                        commandParams.put("databaseSystem", databaseSystem);
                        commandParams.put("destinationHost", destinationHost);
                        command1.setParameters(commandParams);

                        ReplicationThread replicationThread = new ReplicationThread(command1,
                                catalogConnection,
                                logConnection,
                                qosdbcService,
                                qosdbcService.getLoadBalancer());
                        replicationThread.start();
                        result.setState(CommandCode.STATE_SUCCESS);
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