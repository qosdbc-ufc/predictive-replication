/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.terminal.commands;

import qosdbc.commons.data_structure.command.Command;
import qosdbc.commons.data_structure.command.CommandCode;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public final class ExitCommand {

    public static final Command validadeCommand(String clientCommand) {
        Command command = new Command();
        command.setCode(CommandCode.TERMINAL_EXIT);
        return command;
    }
}
