/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.terminal.commands;

import java.util.HashMap;
import qosdbc.commons.OutputMessage;
import qosdbc.commons.command.Command;
import qosdbc.commons.command.CommandCode;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public final class MigrateCommand {

    public static final Command validadeCommand(String clientCommand) {
        Command command = null;
        String[] parameters = clientCommand.split(" ");
        if (parameters != null && parameters.length == 5) {
            String sourceHost = parameters[1];
            String databaseName = parameters[2];
            String databaseSystem = parameters[3];
            String destinationHost = parameters[4];
            try {
                Integer.parseInt(databaseSystem);
            } catch (Exception e) {
                OutputMessage.println("ERROR: Error in formating of the migrate command. Try again!");
                return command;
            }
            command = new Command();
            command.setCode(CommandCode.TERMINAL_MIGRATE);
            HashMap<String, Object> commandParams = new HashMap<String, Object>();
            commandParams.put("sourceHost", sourceHost);
            commandParams.put("databaseName", databaseName);
            commandParams.put("databaseSystem", databaseSystem);
            commandParams.put("destinationHost", destinationHost);
            command.setParameters(commandParams);
            return command;
        } else {
            OutputMessage.println("ERROR: Error in formating of the migrate command. Try again!");
            return command;
        }
    }
}
