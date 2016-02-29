/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.commons.command;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public final class CommandCode {

    public static final int STATE_FAILURE = 0;
    public static final int STATE_SUCCESS = 1;
    
    public static final int DATABASE_CREATE = 2;
    public static final int DATABASE_RESTORE = 3;
    public static final int DATABASE_DROP = 4;
    public static final int DATABASE_DUMP = 9;
    
    public static final int TERMINAL_MIGRATE = 5;
    public static final int TERMINAL_EXIT = 6;
    public static final int TERMINAL_LIST_ACTIVE_DATABASE = 7;
    public static final int TERMINAL_LIST_ACTIVE_VIRTUAL_MACHINE = 8;

    private CommandCode() {
    }
}