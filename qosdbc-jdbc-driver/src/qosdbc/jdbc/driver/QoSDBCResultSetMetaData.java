package qosdbc.jdbc.driver;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import qosdbc.commons.jdbc.QoSDBCMessage.*;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class QoSDBCResultSetMetaData implements ResultSetMetaData {

    private List<Response.Row> resultSetList;
    
    public QoSDBCResultSetMetaData(List<Response.Row > resultSetList) {
        this.resultSetList = resultSetList;
    }        
    
    @Override
    public int getColumnCount() throws SQLException {
        if (resultSetList == null || resultSetList.size() == 0)
            return 0;
        Response.Row row = resultSetList.get(0);
        return row.getColumnListCount();
    }

    @Override
    public boolean isAutoIncrement(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isCaseSensitive(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isSearchable(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isCurrency(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int isNullable(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isSigned(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getColumnDisplaySize(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getColumnLabel(int i) throws SQLException {
        if (resultSetList == null || resultSetList.size() == 0)
            return null;
        Response.Row row = resultSetList.get(0);
        if (i < 1 || i > row.getColumnListCount()) {
            throw new SQLException("Invalid index");
        }
        List<Response.Row.Column> columns = row.getColumnListList();
        for (int j = 0; j < columns.size(); j++) {
            if ((j + 1) == i) {
                return columns.get(j).getColumnLabel();
            }
        }
        return null;
    }

    @Override
    public String getColumnName(int i) throws SQLException {
        if (resultSetList == null || resultSetList.size() == 0)
            return null;
        Response.Row row = resultSetList.get(0);
        if (i < 1 || i > row.getColumnListCount()) {
            throw new SQLException("Invalid index");
        }
        List<Response.Row.Column> columns = row.getColumnListList();
        for (int j = 0; j < columns.size(); j++) {
            if ((j + 1) == i) {
                return columns.get(j).getColumnLabel();
            }
        }
        return null;
    }

    @Override
    public String getSchemaName(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getPrecision(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getScale(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getTableName(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getCatalogName(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getColumnType(int i) throws SQLException {
        if (resultSetList == null || resultSetList.size() == 0)
            return java.sql.Types.NULL;
        Response.Row row = resultSetList.get(0);
        if (i < 1 || i > row.getColumnListCount()) {
            throw new SQLException("Invalid index");
        }
        Object result = null;
        List<Response.Row.Column> columns = row.getColumnListList();
        for (int j = 0; j < columns.size(); j++) {
            if ((j + 1) == i) {
                result = columns.get(j).getColumnValue();
                break;
            }
        }
        if (result != null) {
            if (result instanceof String) {
                return java.sql.Types.VARCHAR;
            }
            if (result instanceof Integer) {
                return java.sql.Types.INTEGER;
            }
            if (result instanceof Float) {
                return java.sql.Types.FLOAT;
            }
            if (result instanceof Double) {
                return java.sql.Types.DOUBLE;
            }
            if (result instanceof Character) {
                return java.sql.Types.CHAR;
            }
            if (result instanceof Boolean) {
                return java.sql.Types.BOOLEAN;
            }
            if (result instanceof java.sql.Date) {
                return java.sql.Types.DATE;
            }
            if (result instanceof java.sql.Time) {
                return java.sql.Types.TIME;
            }
            if (result instanceof java.sql.Timestamp) {
                return java.sql.Types.TIMESTAMP;
            }
        }
        return java.sql.Types.NULL;
    }

    @Override
    public String getColumnTypeName(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isReadOnly(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isWritable(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isDefinitelyWritable(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getColumnClassName(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}