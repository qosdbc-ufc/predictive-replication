/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.commons.jdbc;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class Row implements Serializable {
    
    private List<Column> columnList;
    
    public Row() {
        columnList = new ArrayList<Column>();
    }

    public List<Column> getColumns() {
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }
    
    public void addColumn(Column column) {
        columnList.add(column);
    }
    
    public int size() {
        return columnList.size();
    }
    
    public Object getColumnValueByColumnLabel(String columnLabel) throws SQLException {
        boolean found = false;
        Object result = null;
        for (Column c : columnList) {
            if (c.getColumnLabel().equalsIgnoreCase(columnLabel)) {
                result = c.getColumnValue();
                found = true;
                break;
            }
        }
        if (!found) {
            throw new SQLException("Column not found (" + columnLabel + "). All column labels: " + getAllColumnLabels());
        }
        return result;
    }
    
    public String getAllColumnLabels() {
        String result = "";
        for (Column c : columnList) {
            result += c.getColumnLabel() + ", ";
        }
        if (result.endsWith(", ")) {
            result = result.substring(0, result.length() - ", ".length());
        }
        return result;
    }
    
}