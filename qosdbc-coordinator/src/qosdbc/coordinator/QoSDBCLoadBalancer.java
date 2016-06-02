/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import qosdbc.commons.OutputMessage;

/**
 * @author serafim
 */
public class QoSDBCLoadBalancer {

  // dbName => list(QoSDBCDatabaseProxy)
  private HashMap<String, List<QoSDBCDatabaseProxy>> tenantMap = null;
  // dbName => currentTarget
  private HashMap<String, Integer> targetMap = null;

  public QoSDBCLoadBalancer() {
    tenantMap = new HashMap<String, List<QoSDBCDatabaseProxy>>();
    targetMap = new HashMap<String, Integer>();
  }

  synchronized public QoSDBCDatabaseProxy getTarget(String dbName) {
    if(!IsValidTenant(dbName)) return null;
    if (!tenantMap.containsKey(dbName)) {
      OutputMessage.println("[LoadBalancer] ERROR - There is no dbName = "
              + dbName + " monitored. Could not get connection to it.");
      return null;
    }
    List<QoSDBCDatabaseProxy> connectionList = tenantMap.get(dbName);
    if (connectionList.size() == 1) {
      targetMap.put(dbName, 0);
      return connectionList.get(0);
    }
    int currentIndex = targetMap.get(dbName);
    if (currentIndex >= connectionList.size()-1) {
      currentIndex = 0;
    } else {
      currentIndex++;
    }
    targetMap.put(dbName, currentIndex);
    return connectionList.get(currentIndex);
  }

  synchronized public void addTenant(String dbName, QoSDBCDatabaseProxy conn) {
    if(!IsValidTenant(dbName)) return;
    if(!tenantMap.containsKey(dbName)) {
      List<QoSDBCDatabaseProxy> connectionList = new ArrayList<QoSDBCDatabaseProxy>();

      connectionList.add(conn);
      targetMap.put(dbName, 0);
      tenantMap.put(dbName, connectionList);
      OutputMessage.println("[LoadBalancer] Added Tenant "
              + dbName);

    } else {
      addReplica(dbName, conn);
    }
  }

  synchronized public void addReplica(String dbName, QoSDBCDatabaseProxy conn) {
    if(!IsValidTenant(dbName)) return;
    if (tenantMap.containsKey(dbName)) {
      List<QoSDBCDatabaseProxy> connectionList = tenantMap.get(dbName);

      OutputMessage.println("[LoadBalancer] Added Replica "
              + dbName);
      connectionList.add(conn);
      OutputMessage.println("[LoadBalancer] connectionList size:  " + connectionList.size() + " "
              + dbName);

    }
  }

  synchronized public void removeReplica(String dbName, QoSDBCDatabaseProxy conn) {
    if (tenantMap.containsKey(dbName)) {
      List<QoSDBCDatabaseProxy> connectionList = tenantMap.get(dbName);

      OutputMessage.println("[LoadBalancer] Removing connection: " + conn.getVmId() + "/" + conn.getDbName() + " left: " + connectionList.size());
      connectionList.remove(conn);
      conn.close();
      if (connectionList.isEmpty()) {

        tenantMap.remove(dbName);

        OutputMessage.println("[LoadBalancer] Removing from tenantMap: " + tenantMap.size());
      }

    }
  }

  synchronized public void removeReplica(String dbName) {
    if (tenantMap.containsKey(dbName)) {
      List<QoSDBCDatabaseProxy> connectionList = tenantMap.get(dbName);

      QoSDBCDatabaseProxy conn = connectionList.remove(connectionList.size()-1);
      OutputMessage.println("[LoadBalancer] Removing connection: " + conn.getVmId() + "/" + conn.getDbName() + " left: " + connectionList.size());
      conn.close();
      if (connectionList.isEmpty()) {

        tenantMap.remove(dbName);

        OutputMessage.println("[LoadBalancer] Removing from tenantMap: " + tenantMap.size());
      }

    }
  }


  synchronized public void removeAllReplicas(String dbName) {
    if (tenantMap.containsKey(dbName)) {
      List<QoSDBCDatabaseProxy> connectionList = tenantMap.get(dbName);

      for (QoSDBCDatabaseProxy conn : connectionList) {
        conn.close();
      }
      connectionList.clear();

    }
  }

  synchronized public void removeTenant(String dbName) {
    if (tenantMap.containsKey(dbName)) {
      tenantMap.remove(dbName);
    }
  }

  synchronized public boolean isAutoCommit(String dbName) {
    if (tenantMap.containsKey(dbName)) {
      try {
        return tenantMap.get(dbName).get(0).getConnection().getAutoCommit();
      } catch (SQLException e) {
        OutputMessage.println("[LoadBalancer] ERROR - While getting autoCommit from  "
                + dbName);
        e.printStackTrace();
      }
    }
    return true;
  }

  synchronized private boolean IsValidTenant(String dbName) {
    return !dbName.equals("information_schema")
            && !dbName.equals("mysql")
            && !dbName.equals("performance_schema");
  }
}
