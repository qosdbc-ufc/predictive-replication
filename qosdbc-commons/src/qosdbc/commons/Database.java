/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.commons;

import java.io.Serializable;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class Database implements Serializable {

    private int type;
    private String name;

    public Database(String name, int type) {
        this.name = name;
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public String toString() {
        switch (type) {
            case DatabaseSystem.TYPE_MYSQL: {
                return getName();
            }
            case DatabaseSystem.TYPE_POSTGRES: {
                return getName();
            }
        }
        return super.toString();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}