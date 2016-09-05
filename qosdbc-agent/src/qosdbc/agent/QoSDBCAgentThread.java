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
            OutputMessage.println("QoSDBCAgentThread: Closing client connection: Error " + ex.getMessage());
            proceed = false;
        }
        OutputMessage.println("QoSDBCAgentThread started");
        while (proceed && socket != null && socket.isConnected()) {
            try {
                Object message = inputStream.readObject();
                Command command = (Command) message;
                OutputMessage.println("Request type: " + command.getCode());
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
                        outputStream.flush();
                        break;
                    }
                    case CommandCode.DATABASE_RESTORE: {
                        String databaseName = (String) command.getParameterValue("databaseName");
                        String username = (String) command.getParameterValue("username");
                        String password = (String) command.getParameterValue("password");
                        int databaseType = Integer.parseInt(String.valueOf(command.getParameterValue("databaseType")));
                        Database database = new Database(databaseName, databaseType);
                        /* Database restore process */
                        OutputMessage.println("DATABASE_RESTORE: dbTpe= " + databaseType + " - " + databaseName + " - " + username  + " - " + password);
                        String dumpFileURL = (String) command.getParameterValue("dumpFileUrl");
                        OutputMessage.println("DumpFile URL: " + dumpFileURL);
                        File dumpFile = ShellCommand.downloadFile(dumpFileURL, dumpFileURL.substring(dumpFileURL.lastIndexOf("/") + 1));
                        OutputMessage.println("dumpFile object = " + dumpFile.getAbsolutePath());
                        boolean dumpSuccess = ShellCommand.restoreCompleteDatabase(database, username, password, dumpFile);
                        if (dumpSuccess) {
                            result.setState(CommandCode.STATE_SUCCESS);
                        }
                        outputStream.writeObject(result);
                        outputStream.flush();
                        dumpFile.delete();
                        break;
                    }

                    case CommandCode.DATABASE_SYNC: {
                        OutputMessage.println("DATABASE_SYNC REQUEST");
                        String databaseName = (String) command.getParameterValue("databaseName");
                        String username = (String) command.getParameterValue("username");
                        String password = (String) command.getParameterValue("password");
                        int databaseType = Integer.parseInt(String.valueOf(command.getParameterValue("databaseType")));
                        Database database = new Database(databaseName, databaseType);
                        /* Database restore process */
                        OutputMessage.println("DATABASE_SYNC: dbTpe= " + databaseType + " - " + databaseName + " - " + username  + " - " + password);
                        String dumpFileURL = (String) command.getParameterValue("syncFileUrl");
                        OutputMessage.println("syncFile URL: " + dumpFileURL);
                        File syncFile = ShellCommand.downloadFile(dumpFileURL, dumpFileURL.substring(dumpFileURL.lastIndexOf("/") + 1));
                        OutputMessage.println("syncFile object = " + syncFile.getAbsolutePath());
                        boolean dumpSuccess = ShellCommand.syncDatabase(database, username, password, syncFile);
                        if (dumpSuccess) {
                            result.setState(CommandCode.STATE_SUCCESS);
                        }
                        outputStream.writeObject(result);
                        outputStream.flush();
                        syncFile.delete();
                        break;
                    }

                    case CommandCode.DATABASE_DUMP: {
                        String databaseName = (String) command.getParameterValue("databaseName");
                        String username = (String) command.getParameterValue("username");
                        String password = (String) command.getParameterValue("password");
                        int databaseType = Integer.parseInt(String.valueOf(command.getParameterValue("databaseType")));
                        Database database = new Database(databaseName, databaseType);
                        /* Database dump process */
                        OutputMessage.println("Dump info: dbTpe= " + databaseType + " - " + databaseName + " - " + username  + " - " + password);
                        File dumpFile = null;
                        //File dest = null;
                        try {
                            dumpFile = ShellCommand.dumpCompleteDatabase(database, username, password);
                            //dest = new File("/var/www/html/qosdbc");
                            //OutputMessage.println("Copying dump... " + dumpFile.getAbsolutePath() );
                            //OutputMessage.println("to " + dest.getAbsolutePath());
                        }catch (Exception e) {
                            OutputMessage.println("ERROR: DD - " + e.getMessage());
                            e.printStackTrace();
                        }

                        if (dumpFile != null) {
                            /*try {
                                FileUtils.copyFileToDirectory(dumpFile, dest);
                            }catch (Exception e) {
                                OutputMessage.println("ERROR: DD - " + e.getMessage());
                                e.printStackTrace();
                            }*/
                            //OutputMessage.println("Dump copied Success: " + "http://" + vmId + "/qosdbc/" + dumpFile.getName());
                            result.setResultObject("http://" + vmId + "/qosdbc/" + dumpFile.getName());
                            result.setState(CommandCode.STATE_SUCCESS);
                        }
                        outputStream.writeObject(result);
                        outputStream.flush();
                        //dumpFile.delete();
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
                        outputStream.flush();
                        break;
                    }
                }
            } catch (ClassNotFoundException ex) {
                ex.printStackTrace();
                break;
            } catch (IOException ex) {
                ex.printStackTrace();
                break;
            }
        }
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        OutputMessage.println("QoSDBCAgentThread ended");
    }
}