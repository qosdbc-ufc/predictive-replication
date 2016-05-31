/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.sql.Connection;
import java.util.*;
import qosdbc.commons.OutputMessage;

/**
 * @author serafim
 */
public class QoSDBCLoadBalancer {
    
  // dbName => list(jdbc connections)
  private HashMap<String, List<Connection>> tenantMap = null;
  // dbName => currentTarget
  private HashMap<String, Integer> targetMap = null;
    
  public QoSDBCLoadBalancer() {
    tenantMap = new HashMap<String, List<Connection>>();
    targetMap = new HashMap<String, Integer>();
  }

  public Connection getTarget(String dbName) {
    if (!tenantMap.containsKey(dbName)) {
      OutputMessage.println("[LoadBalancer] ERROR - There is no dbName = "
          + dbName + " monitored. Could not get connection to it.");
      return null;
    }
    List<Connection> connectionList = tenantMap.get(dbName);
    if (connectionList.size() == 1) {
      targetMap.put(dbName, 0);
      return connectionList.get(0);
    }
    int currentIndex = targetMap.get(dbName);
    if (currentIndex == connectionList.size()-1) {
      currentIndex = 0;
    } else {
      currentIndex++;
    }
    targetMap.put(dbName, currentIndex);
    return connectionList.get(currentIndex);
  }

  public void addTenant(String dbName, Connection conn) {
    if(!tenantMap.containsKey(dbName)) {
      List<Connection> connectionList = new ArrayList<Connection>();
      synchronized (connectionList) {
        connectionList.add(conn);
      }
    }
  }

  public void addReplica(String dbName, Connection conn) {
    if (tenantMap.containsKey(dbName)) {
        List<Connection> connectionList = tenantMap.get(dbName);
        synchronized (connectionList) {
          connectionList.add(conn);
        }
    }
  }

  public void removeReplica(String dbName, Connection conn) {
    if (tenantMap.containsKey(dbName)) {
      List<Connection> connectionList = tenantMap.get(dbName);
      synchronized (connectionList) {
        connectionList.remove(conn);
      }
    }
  }
}
