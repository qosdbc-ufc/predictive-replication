/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.jdbc.driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import qosdbc.commons.jdbc.Request;
import qosdbc.commons.jdbc.RequestCode;
import qosdbc.commons.jdbc.Response;
import qosdbc.commons.jdbc.Row;

/**
 * 
 * @author Leonardo Oliveira Moreira
 * 
 */
public class QoSDBCStatement implements Statement {

    private QoSDBCConnection connection;
    
    protected Hashtable<Integer, Object> parameters;
    protected List<Hashtable<Integer, Object>> batchParamList;
    protected String sql;
    
    private long id;
    
    public long getStatementID() {
        return id;
    }

    public QoSDBCStatement(QoSDBCConnection connection) {
        this.connection = connection;
        this.id = System.currentTimeMillis();
    }
    
    private String treatSQL(String sql) {
        if (sql == null || sql.trim().length() == 0) {
            return "";
        }
        char[] t = sql.toCharArray();
        sql = "";
        for (char c : t) {
            if (c == '\\') {
                sql += "\\\\";
                continue;
            }
            if (c == '\'') {
                sql += "\\'";
                continue;
            }
            sql += c;
        }
        return sql;
    }

    /**
     * 
     * @param sql
     * @return 
     */
    protected String replaceParameters(String sql, Hashtable<Integer, Object> parameters) {
        String result = "";
        char[] s = sql.toCharArray();
        int parameterIndex = 1;
        for (char c : s) {
            if (c == '?') {
                Object o = parameters.get(parameterIndex);
                parameterIndex++;
                if (o instanceof String) {
                    result = result + "'" + treatSQL(String.valueOf(o)) + "'";
                    continue;
                }
                if (o instanceof Character) {
                    result = result + "'" + treatSQL(String.valueOf(o)) + "'";
                    continue;
                }
                if (o instanceof java.sql.Timestamp) {
                    result = result + "'" + String.valueOf(o) + "'";
                    continue;
                }
                if (o instanceof java.sql.Time) {
                    result = result + "'" + String.valueOf(o) + "'";
                    continue;
                }
                if (o instanceof java.sql.Date) {
                    result = result + "'" + String.valueOf(o) + "'";
                    continue;
                }
                result = result + "" + String.valueOf(o) + "";
            }
            else {
                result = result + String.valueOf(c);
            }
        }
        return result;
    }

    @Override
    public synchronized ResultSet executeQuery(String sql) throws SQLException {
        // @gabiarra - for ycsb oltpbenchmark - start
        if (sql != null && sql.indexOf(".USERTABLE.") != -1) {
            sql = sql.replaceAll(".USERTABLE.", "USERTABLE");
        }
        // @gabiarra - for ycsb oltpbenchmark - end
        Request request = new Request();
        request.setCode(RequestCode.SQL_RESULTSET_CREATE);
        request.addParameter("statementId", getStatementID());
        request.addParameter("resultSetId", System.currentTimeMillis());
        request.setCommand(sql);
        request.setDatabase(connection.getDatabaseName());
        Response response = connection.executeMessage(request);
        List<Row> resultSetList = (List<Row>) response.getResultObject();
        if (resultSetList == null || response.getState() != RequestCode.STATE_SUCCESS) {
            throw new SQLException("Invalid query");
        }
        QoSDBCResultSet resultSet = new QoSDBCResultSet(resultSetList, connection, Long.parseLong(request.getParameterValue("resultSetId").toString()), getStatementID());
        return resultSet;
    }

    @Override
    public synchronized int executeUpdate(String sql) throws SQLException {
        Request request = new Request();
        if (sql.toLowerCase().startsWith("insert")) {
            request.setCode(RequestCode.SQL_UPDATE);
        }
        if (sql.toLowerCase().startsWith("update")) {
            request.setCode(RequestCode.SQL_UPDATE);
        }
        if (sql.toLowerCase().startsWith("delete")) {
            request.setCode(RequestCode.SQL_UPDATE);
        }
        request.addParameter("statementId", getStatementID());
        request.setCommand(sql);
        request.setDatabase(connection.getDatabaseName());
        Response response = connection.executeMessage(request);
        long result = Long.parseLong(response.getResultObject().toString());
        if (result < 0 || response == null || response.getState() != RequestCode.STATE_SUCCESS) {
            throw new SQLException("Invalid query");
        }
        return (int) result;
    }

    @Override
    public synchronized void close() throws SQLException {
        Request request = new Request();
        request.setCode(RequestCode.SQL_STATEMENT_CLOSE);
        request.addParameter("statementId", getStatementID());
        request.setDatabase(connection.getDatabaseName());
        Response response = connection.executeMessage(request);
        if (response == null || response.getState() == RequestCode.STATE_FAILURE) {
            throw new SQLException("Failed to close statement");
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void cancel() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public synchronized boolean execute(String sql) throws SQLException {
        Request request = new Request();
        sql = sql.trim();
        request.setDatabase(connection.getDatabaseName());
        request.addParameter("statementId", getStatementID());
        request.setCommand(sql);
        request.setCode(RequestCode.SQL_UPDATE);
        if (sql.toLowerCase().startsWith("select")) {
            request.setCode(RequestCode.SQL_RESULTSET_CREATE);
        }
        if (sql.toLowerCase().startsWith("insert")) {
            request.setCode(RequestCode.SQL_UPDATE);
        }
        if (sql.toLowerCase().startsWith("update")) {
            request.setCode(RequestCode.SQL_UPDATE);
        }
        if (sql.toLowerCase().startsWith("delete")) {
            request.setCode(RequestCode.SQL_UPDATE);
        }
        Response response = connection.executeMessage(request);
        return (response != null && Long.parseLong(response.getResultObject().toString()) >= 0);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void clearBatch() throws SQLException {
        batchParamList = new ArrayList<Hashtable<Integer, Object>>();
    }

    @Override
    public synchronized int[] executeBatch() throws SQLException {
        int[] result = new int[batchParamList.size()];
        for (int i = 0; i < batchParamList.size(); i++) {
            Hashtable<Integer, Object> params = batchParamList.get(i);
            String sqlTemp = replaceParameters(sql, params);
            result[i] = (execute(sqlTemp) ? SUCCESS_NO_INFO : EXECUTE_FAILED);
        }
        return result;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}