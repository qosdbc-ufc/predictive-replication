/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.agent;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.util.List;
import org.apache.commons.io.FileUtils;
import qosdbc.commons.Database;
import qosdbc.commons.DatabaseSystem;
import qosdbc.commons.OutputMessage;
import qosdbc.commons.ShellCommand;
import qosdbc.commons.command.Command;
import qosdbc.commons.command.CommandCode;
import qosdbc.commons.command.Return;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class QoSDBCAgentThread extends Thread {

    private Socket socket;
    private ObjectOutputStream outputStream;
    private ObjectInputStream inputStream;
    private Connection catalogConnection;
    private Connection logConnection;
    private List<DatabaseSystem> databaseSystems;
    private String vmId;

    public QoSDBCAgentThread(Socket socket, Connection catalogConnection, Connection logConnection, List<DatabaseSystem> databaseSystems, String vmId) {
        this.socket = socket;
        this.vmId = vmId;
        this.catalogConnection = catalogConnection;
        this.logConnection = logConnection;
        this.databaseSystems = databaseSystems;
    }

    @Override
    public void run() {
        OutputMessage.println("QoSDBCAgentThread starting");
        boolean proceed = true;
        try {
            outputStream = new ObjectOutputStream((socket.getOutputStream()));
            inputStream = new ObjectInputStream((socket.getInputStream()));
        } catch (IOException ex) {
            OutputMessage.println("QoSDBCAgentThread: Closing client connection");
            proceed = false;
        }
        OutputMessage.println("QoSDBCAgentThread started");
        while (proceed && socket != null && socket.isConnected()) {
            try {
                Object message = inputStream.readObject();
                Command command = (Command) message;
                Return result = new Return();
                result.setState(CommandCode.STATE_FAILURE);
                switch (command.getCode()) {
                    case CommandCode.DATABASE_CREATE: {
                        String databaseName = (String) command.getParameterValue("databaseName");
                        String username = (String) command.getParameterValue("username");
                        String password = (String) command.getParameterValue("password");
                        int databaseType = Integer.parseInt(String.valueOf(command.getParameterValue("databaseType")));
                        Database database = new Database(databaseName, databaseType);
                        /* Database create process */
                        boolean createSuccess = ShellCommand.createDatabase(database, username, password);
                        if (createSuccess) {
                            result.setState(CommandCode.STATE_SUCCESS);
                        }
                        outputStream.writeObject(result);
                        outputStream.reset();
                        break;
                    }
                    case CommandCode.DATABASE_RESTORE: {
                        String databaseName = (String) command.getParameterValue("databaseName");
                        String username = (String) command.getParameterValue("username");
                        String password = (String) command.getParameterValue("password");
                        int databaseType = Integer.parseInt(String.valueOf(command.getParameterValue("databaseType")));
                        Database database = new Database(databaseName, databaseType);
                        /* Database restore process */
                        String dumpFileURL = (String) command.getParameterValue("dumpFileURL");
                        File dumpFile = ShellCommand.downloadFile(dumpFileURL, dumpFileURL.substring(dumpFileURL.lastIndexOf("/") + 1));
                        boolean dumpSuccess = ShellCommand.restoreCompleteDatabase(database, username, password, dumpFile);
                        if (dumpSuccess) {
                            result.setState(CommandCode.STATE_SUCCESS);
                        }
                        outputStream.writeObject(result);
                        outputStream.reset();
                        dumpFile.delete();
                        break;
                    }
                    case CommandCode.DATABASE_DUMP: {
                        String databaseName = (String) command.getParameterValue("databaseName");
                        String username = (String) command.getParameterValue("username");
                        String password = (String) command.getParameterValue("password");
                        int databaseType = Integer.parseInt(String.valueOf(command.getParameterValue("databaseType")));
                        Database database = new Database(databaseName, databaseType);
                        /* Database dump process */
                        File dumpFile = ShellCommand.dumpCompleteDatabase(database, username, password);
                        if (dumpFile != null) {
                            FileUtils.copyFileToDirectory(dumpFile, new File("/var/www/qosdbc/"));
                            result.setResultObject("http://" + vmId + "/qosdbc/" + dumpFile.getName());
                            result.setState(CommandCode.STATE_SUCCESS);
                        }
                        outputStream.writeObject(result);
                        outputStream.reset();
                        dumpFile.delete();
                        break;
                    }
                    case CommandCode.DATABASE_DROP: {
                        String databaseName = (String) command.getParameterValue("databaseName");
                        String username = (String) command.getParameterValue("username");
                        String password = (String) command.getParameterValue("password");
                        int databaseType = Integer.parseInt(String.valueOf(command.getParameterValue("databaseType")));
                        Database database = new Database(databaseName, databaseType);
                        boolean dropDatabase = ShellCommand.dropDatabase(database, username, password);
                        if (dropDatabase) {
                            result.setState(CommandCode.STATE_SUCCESS);
                        } else {
                            OutputMessage.println("ERROR: " + "unable to remove the database");
                        }
                        outputStream.writeObject(result);
                        outputStream.reset();
                        break;
                    }
                }
            } catch (ClassNotFoundException ex) {
                break;
            } catch (IOException ex) {
                break;
            }
        }
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException ex) {
            }
        }
        OutputMessage.println("QoSDBCAgentThread ended");
    }
}