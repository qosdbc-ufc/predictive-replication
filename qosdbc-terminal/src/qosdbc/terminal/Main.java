/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.terminal;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import qosdbc.commons.OutputMessage;
import qosdbc.commons.command.Command;
import qosdbc.commons.command.CommandCode;
import qosdbc.commons.command.Return;
import qosdbc.terminal.commands.ExitCommand;
import qosdbc.terminal.commands.ListActiveDatabaseCommand;
import qosdbc.terminal.commands.ListActiveVirtualMachineCommand;
import qosdbc.terminal.commands.MigrateCommand;

/**
 *
 * @author leoomoreira
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String line = "";
        for (int i = 0; i < 80; i++) {
            line += "*";
        }
        OutputMessage.println(line, false);
        OutputMessage.println("### QoSDBC Terminal v.0.1", false);
        OutputMessage.println(line, false);
        if (args == null || args.length != 2) {
            OutputMessage.println("Command Sintax......: java -jar qosdbc-terminal.jar <qosdbc_host> <qosdbc_terminal_port>", false);
            OutputMessage.println("Command Example.....: java -jar qosdbc-terminal.jar qosdbc.sever 7779", false);
        } else {
            String qosdbcHostParam = args[0];
            String qosdbcTerminalPortParam = args[1];
            OutputMessage.println("Parameters: ", false);
            OutputMessage.println(line, false);
            OutputMessage.println("QoSDBC Host..............: " + qosdbcHostParam, false);
            OutputMessage.println("QoSDBC Terminal Port.....: " + qosdbcTerminalPortParam, false);
            OutputMessage.println(line, false);

            int qosdbcTerminalPort = -1;
            try {
                qosdbcTerminalPort = Integer.parseInt(qosdbcTerminalPortParam);
            } catch (Exception ex) {
                OutputMessage.println("ERROR: " + qosdbcTerminalPortParam + " is not a port valid");
                System.exit(0);
            }

            try {
                OutputMessage.println("[" + "Terminal" + "]: connection starting");
                Socket socket = new Socket(qosdbcHostParam, qosdbcTerminalPort);
                ObjectOutputStream outputStream = null;
                ObjectInputStream inputStream = null;
                boolean proceed = true;
                try {
                    outputStream = new ObjectOutputStream((socket.getOutputStream()));
                    inputStream = new ObjectInputStream((socket.getInputStream()));
                } catch (IOException ex) {
                    OutputMessage.println("[" + "Terminal" + "]: Closing connection");
                    proceed = false;
                }
                Scanner scanner = new Scanner(System.in);
                if (proceed && socket != null && socket.isConnected()) {
                    OutputMessage.println("[" + "Terminal" + "]: connection started");
                }
                while (proceed && socket != null && socket.isConnected()) {
                    String clientCommand = null;
                    Command command = null;
                    do {
                        OutputMessage.println(line, false);
                        OutputMessage.println("Commands: ", false);
                        OutputMessage.println(line, false);
                        OutputMessage.println("$ migrate <source host> <database_name> <database_system> <destination_host>", false);
                        OutputMessage.println("$ list_active_database", false);
                        OutputMessage.println("$ list_active_virtual_machine", false);
                        OutputMessage.println("$ exit", false);
                        OutputMessage.println(line, false);
                        OutputMessage.print("$ ", false);
                        clientCommand = scanner.nextLine();
                        if (clientCommand != null) {
                            clientCommand = treatCommand(clientCommand);
                        }
                        if (clientCommand != null && clientCommand.trim().length() > 0) {
                            String[] completeCommand = clientCommand.split(" ");
                            if (completeCommand == null) {
                                completeCommand = new String[]{clientCommand};
                            }

                            String mainCommand = completeCommand[0];
                            // live migration command
                            if (mainCommand.equalsIgnoreCase("migrate")) {
                                command = MigrateCommand.validadeCommand(clientCommand);
                                if (command == null) {
                                    OutputMessage.println("ERROR: Error in formating of the command. Try again!");
                                    continue;
                                }
                            }
                            // list active database command
                            if (mainCommand.equalsIgnoreCase("list_active_database")) {
                                command = ListActiveDatabaseCommand.validadeCommand(clientCommand);
                                if (command == null) {
                                    OutputMessage.println("ERROR: Error in formating of the command. Try again!");
                                    continue;
                                }
                            }
                            // list active virtual machine command
                            if (mainCommand.equalsIgnoreCase("list_active_virtual_machine")) {
                                command = ListActiveVirtualMachineCommand.validadeCommand(clientCommand);
                                if (command == null) {
                                    OutputMessage.println("ERROR: Error in formating of the command. Try again!");
                                    continue;
                                }
                            }
                            // exit terminal command
                            if (mainCommand.equalsIgnoreCase("exit")) {
                                command = ExitCommand.validadeCommand(clientCommand);
                                if (command == null) {
                                    OutputMessage.println("ERROR: Error in formating of the command. Try again!");
                                    continue;
                                }
                            }
                        }
                    } while (clientCommand == null || clientCommand.trim().length() == 0 || command == null);

                    if (command.getCode() == CommandCode.TERMINAL_EXIT) {
                        socket.close();
                        break;
                    }

                    outputStream.writeObject(command);
                    outputStream.reset();

                    Object object = inputStream.readObject();
                    Return result = (Return) object;

                    switch (result.getState()) {
                        case CommandCode.STATE_SUCCESS: {
                            OutputMessage.println("The command was executed successfully");
                            OutputMessage.println(result.getResultObject().toString(), false);
                            break;
                        }
                        case CommandCode.STATE_FAILURE: {
                            OutputMessage.println("The command was not executed successfully");
                            OutputMessage.println(result.getResultObject().toString(), false);
                            break;
                        }
                    }
                }
                OutputMessage.println("[" + "Terminal" + "]: connection ended");
                if (socket != null && !socket.isClosed()) {
                    try {
                        socket.close();
                    } catch (IOException ex) {
                    }
                }
            } catch (ClassNotFoundException ex) {
                OutputMessage.println("[" + "Terminal" + "]: ERROR: " + ex.getMessage());
            } catch (IOException ex) {
                OutputMessage.println("[" + "Terminal" + "]: ERROR: " + ex.getMessage());
            }
        }
    }

    public static String treatCommand(String command) {
        String patternStr = "\\s+";
        String replaceStr = " ";
        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(command);
        return matcher.replaceAll(replaceStr);
    }
}