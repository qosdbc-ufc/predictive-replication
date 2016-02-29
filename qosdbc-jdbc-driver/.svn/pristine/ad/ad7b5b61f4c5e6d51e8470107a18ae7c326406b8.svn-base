/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.jdbc.driver;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import qosdbc.commons.jdbc.Column;
import qosdbc.commons.jdbc.Request;
import qosdbc.commons.jdbc.RequestCode;
import qosdbc.commons.jdbc.Response;
import qosdbc.commons.jdbc.Row;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class QoSDBCResultSet implements ResultSet {

    private List<Row> resultSetList;
    private int indexCurrent = -1;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S");
    private QoSDBCResultSetMetaData resultSetMetaData;
    private QoSDBCConnection connection;
    private long resultSetID;
    private long statementID;

    public long getStatementID() {
        return statementID;
    }

    private long getResultSetID() {
        return resultSetID;
    }

    public QoSDBCResultSet(List<Row> resultSetList, QoSDBCConnection connection, long resultSetID, long statementID) throws SQLException {
        this.resultSetList = resultSetList;
        this.connection = connection;
        this.resultSetID = resultSetID;
        this.statementID = statementID;
        indexCurrent = -1;
    }

    @Override
    public synchronized boolean next() throws SQLException {
        if (indexCurrent >= resultSetList.size() - 1) {
            return false;
        }
        indexCurrent++;
        return true;
    }

    @Override
    public void close() throws SQLException {
        this.resultSetList = new ArrayList<Row>();
        indexCurrent = -1;
        Request request = new Request();
        request.setCode(RequestCode.SQL_RESULTSET_CLOSE);
        request.addParameter("resultSetId", getResultSetID());
        request.addParameter("statementId", getStatementID());
        request.setDatabase(connection.getDatabaseName());
        Response response = connection.executeMessage(request);
        if (response == null || response.getState() == RequestCode.STATE_FAILURE) {
            throw new SQLException("Failed to close result");
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public synchronized String getString(int columnIndex) throws SQLException {
        Object result = null;
        try {
            Row row = resultSetList.get(indexCurrent);
            if (columnIndex < 1 || columnIndex > row.size()) {
                throw new SQLException("Invalid index");
            }
            List<Column> columns = row.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if ((i + 1) == columnIndex) {
                    result = columns.get(i).getColumnValue();
                    break;
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        if (result == null) {
            return null;
        }
        return String.valueOf(result);
    }

    @Override
    public synchronized boolean getBoolean(int columnIndex) throws SQLException {
        Object result = null;
        try {
            Row row = resultSetList.get(indexCurrent);
            if (columnIndex < 1 || columnIndex > row.size()) {
                throw new SQLException("Invalid index");
            }
            List<Column> columns = row.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if ((i + 1) == columnIndex) {
                    result = columns.get(i).getColumnValue();
                    break;
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        if (result == null) {
            return false;
        }
        return Boolean.parseBoolean(String.valueOf(result));
    }

    @Override
    public synchronized byte getByte(int columnIndex) throws SQLException {
        Object result = null;
        try {
            Row row = resultSetList.get(indexCurrent);
            if (columnIndex < 1 || columnIndex > row.size()) {
                throw new SQLException("Invalid index");
            }
            List<Column> columns = row.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if ((i + 1) == columnIndex) {
                    result = columns.get(i).getColumnValue();
                    break;
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        if (result == null) {
            return 0;
        }
        return Byte.parseByte(String.valueOf(result));
    }

    @Override
    public synchronized short getShort(int columnIndex) throws SQLException {
        Object result = null;
        try {
            Row row = resultSetList.get(indexCurrent);
            if (columnIndex < 1 || columnIndex > row.size()) {
                throw new SQLException("Invalid index");
            }
            List<Column> columns = row.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if ((i + 1) == columnIndex) {
                    result = columns.get(i).getColumnValue();
                    break;
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        if (result == null) {
            return 0;
        }
        return Short.parseShort(String.valueOf(result));
    }

    @Override
    public synchronized int getInt(int columnIndex) throws SQLException {
        Object result = null;
        try {
            Row row = resultSetList.get(indexCurrent);
            if (columnIndex < 1 || columnIndex > row.size()) {
                throw new SQLException("Invalid index");
            }
            List<Column> columns = row.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if ((i + 1) == columnIndex) {
                    result = columns.get(i).getColumnValue();
                    break;
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        if (result == null) {
            return 0;
        }
        return Integer.parseInt(String.valueOf(result));
    }

    @Override
    public synchronized long getLong(int columnIndex) throws SQLException {
        Object result = null;
        try {
            Row row = resultSetList.get(indexCurrent);
            if (columnIndex < 1 || columnIndex > row.size()) {
                throw new SQLException("Invalid index");
            }
            List<Column> columns = row.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if ((i + 1) == columnIndex) {
                    result = columns.get(i).getColumnValue();
                    break;
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        if (result == null) {
            return 0;
        }
        return Long.parseLong(String.valueOf(result));
    }

    @Override
    public synchronized float getFloat(int columnIndex) throws SQLException {
        Object result = null;
        try {
            Row row = resultSetList.get(indexCurrent);
            if (columnIndex < 1 || columnIndex > row.size()) {
                throw new SQLException("Invalid index");
            }
            List<Column> columns = row.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if ((i + 1) == columnIndex) {
                    result = columns.get(i).getColumnValue();
                    break;
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        if (result == null) {
            return 0;
        }
        return Float.parseFloat(String.valueOf(result));
    }

    @Override
    public synchronized double getDouble(int columnIndex) throws SQLException {
        Object result = null;
        try {
            Row row = resultSetList.get(indexCurrent);
            if (columnIndex < 1 || columnIndex > row.size()) {
                throw new SQLException("Invalid index");
            }
            List<Column> columns = row.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if ((i + 1) == columnIndex) {
                    result = columns.get(i).getColumnValue();
                    break;
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        if (result == null) {
            return 0;
        }
        return Double.parseDouble(String.valueOf(result));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public synchronized Date getDate(int columnIndex) throws SQLException {
        Object result = null;
        try {
            Row row = resultSetList.get(indexCurrent);
            if (columnIndex < 1 || columnIndex > row.size()) {
                throw new SQLException("Invalid index");
            }
            List<Column> columns = row.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if ((i + 1) == columnIndex) {
                    result = columns.get(i).getColumnValue();
                    break;
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        if (result == null) {
            return null;
        }
        java.util.Date d = null;
        try {
            d = dateFormat.parse(String.valueOf(result));
        } catch (ParseException ex) {
            throw new SQLException("Invalid data type");
        }
        return new java.sql.Date(d.getTime());
    }

    @Override
    public synchronized Time getTime(int columnIndex) throws SQLException {
        Object result = null;
        try {
            Row row = resultSetList.get(indexCurrent);
            if (columnIndex < 1 || columnIndex > row.size()) {
                throw new SQLException("Invalid index");
            }
            List<Column> columns = row.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if ((i + 1) == columnIndex) {
                    result = columns.get(i).getColumnValue();
                    break;
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        if (result == null) {
            return null;
        }
        java.util.Date d = null;
        try {
            d = dateFormat.parse(String.valueOf(result));
        } catch (ParseException ex) {
            throw new SQLException("Invalid data type");
        }
        return new java.sql.Time(d.getTime());
    }

    @Override
    public synchronized Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object result = null;
        try {
            Row row = resultSetList.get(indexCurrent);
            if (columnIndex < 1 || columnIndex > row.size()) {
                throw new SQLException("Invalid index");
            }
            List<Column> columns = row.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if ((i + 1) == columnIndex) {
                    result = columns.get(i).getColumnValue();
                    break;
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
        if (result == null) {
            return null;
        }
        java.util.Date d = null;
        try {
            d = dateFormat.parse(String.valueOf(result));
        } catch (ParseException ex) {
            throw new SQLException("Invalid data type");
        }
        return new java.sql.Timestamp(d.getTime());
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public synchronized String getString(String columnLabel) throws SQLException {
        Row row = resultSetList.get(indexCurrent);
        Object result = row.getColumnValueByColumnLabel(columnLabel);
        if (result == null) {
            return null;
        }
        return String.valueOf(result);
    }

    @Override
    public synchronized boolean getBoolean(String columnLabel) throws SQLException {
        Row row = resultSetList.get(indexCurrent);
        Object result = row.getColumnValueByColumnLabel(columnLabel);
        if (result == null) {
            return false;
        }
        return Boolean.parseBoolean(String.valueOf(result));
    }

    @Override
    public synchronized byte getByte(String columnLabel) throws SQLException {
        Row row = resultSetList.get(indexCurrent);
        Object result = row.getColumnValueByColumnLabel(columnLabel);
        if (result == null) {
            return 0;
        }
        return Byte.parseByte(String.valueOf(result));
    }

    @Override
    public synchronized short getShort(String columnLabel) throws SQLException {
        Row row = resultSetList.get(indexCurrent);
        Object result = row.getColumnValueByColumnLabel(columnLabel);
        if (result == null) {
            return 0;
        }
        return Short.parseShort(String.valueOf(result));
    }

    @Override
    public synchronized int getInt(String columnLabel) throws SQLException {
        Row row = resultSetList.get(indexCurrent);
        Object result = row.getColumnValueByColumnLabel(columnLabel);
        if (result == null) {
            return 0;
        }
        return Integer.parseInt(String.valueOf(result));
    }

    @Override
    public synchronized long getLong(String columnLabel) throws SQLException {
        Row row = resultSetList.get(indexCurrent);
        Object result = row.getColumnValueByColumnLabel(columnLabel);
        if (result == null) {
            return 0;
        }
        return Long.parseLong(String.valueOf(result));
    }

    @Override
    public synchronized float getFloat(String columnLabel) throws SQLException {
        Row row = resultSetList.get(indexCurrent);
        Object result = row.getColumnValueByColumnLabel(columnLabel);
        if (result == null) {
            return 0;
        }
        return Float.parseFloat(String.valueOf(result));
    }

    @Override
    public synchronized double getDouble(String columnLabel) throws SQLException {
        Row row = resultSetList.get(indexCurrent);
        Object result = row.getColumnValueByColumnLabel(columnLabel);
        if (result == null) {
            return 0;
        }
        return Double.parseDouble(String.valueOf(result));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public synchronized Date getDate(String columnLabel) throws SQLException {
        Row row = resultSetList.get(indexCurrent);
        Object result = row.getColumnValueByColumnLabel(columnLabel);
        if (result == null) {
            return null;
        }
        String s = String.valueOf(result);
        java.util.Date d;
        try {
            d = dateFormat.parse(s);
        } catch (ParseException ex) {
            d = null;
        }
        return new java.sql.Date(d.getTime());
    }

    @Override
    public synchronized Time getTime(String columnLabel) throws SQLException {
        Row row = resultSetList.get(indexCurrent);
        Object result = row.getColumnValueByColumnLabel(columnLabel);
        if (result == null) {
            return null;
        }
        String s = String.valueOf(result);
        java.util.Date d;
        try {
            d = dateFormat.parse(s);
        } catch (ParseException ex) {
            d = null;
        }
        return new java.sql.Time(d.getTime());
    }

    @Override
    public synchronized Timestamp getTimestamp(String columnLabel) throws SQLException {
        Row row = resultSetList.get(indexCurrent);
        Object result = row.getColumnValueByColumnLabel(columnLabel);
        if (result == null) {
            return null;
        }
        String s = String.valueOf(result);
        java.util.Date d;
        try {
            d = dateFormat.parse(s);
        } catch (ParseException ex) {
            d = null;
        }
        return new java.sql.Timestamp(d.getTime());
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
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
    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new QoSDBCResultSetMetaData(resultSetList);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void afterLast() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean first() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean last() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getRow() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean previous() throws SQLException {
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
    public int getType() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getConcurrency() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Statement getStatement() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
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
    public <T> T getObject(int i, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <T> T getObject(String string, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}