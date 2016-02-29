/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.commons.jdbc;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public final class RequestCode {

    public static final int STATE_FAILURE = 0;
    public static final int STATE_SUCCESS = 1;
    public static final int SQL_COMMIT = 2;
    public static final int SQL_ROLLBACK = 3;
    public static final int SQL_CONNECTION_CREATE = 4;
    public static final int SQL_CONNECTION_CLOSE = 5;
    public static final int SQL_STATEMENT_CREATE = 6;
    public static final int SQL_STATEMENT_CLOSE = 7;
    public static final int SQL_RESULTSET_CREATE = 8;
    public static final int SQL_RESULTSET_CLOSE = 9;
    public static final int SQL_UPDATE = 10;
    public static final int CONNECTION_CHANGE_AUTOCOMMIT = 11;

    private RequestCode() {
    }
}