/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.commons;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class DatabaseSystem implements Serializable {

    public static final int TYPE_POSTGRES = 1;
    public static final int TYPE_MYSQL = 2;
    private int type;
    private List<Database> databaseList;
    private String user;
    private String password;

    public DatabaseSystem() {
        databaseList = new ArrayList<Database>();
    }

    public List<Database> getDatabaseList() {
        return databaseList;
    }

    public void setDatabaseList(List<Database> databaseList) {
        this.databaseList = databaseList;
    }
    
    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
    
    @Override
    public String toString() {
        switch (type) {
            case TYPE_MYSQL: {
                return "MySQL";
            }
            case TYPE_POSTGRES: {
                return "PostgreSQL";
            }
        }
        return super.toString();
    }
}