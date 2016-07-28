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

  // proxyId => list(QoSDBCDatabaseProxy)
  private static HashMap<Long, List<QoSDBCDatabaseProxy>> tenantMap = null;
  // proxyId => currentTarget
  private static HashMap<Long, Integer> targetMap = null;
  // dbName => Replicas
  private static HashMap<String, List<QoSDBCDatabaseProxy>> replicasMap = null;

  public QoSDBCLoadBalancer() {
    tenantMap = new HashMap<Long, List<QoSDBCDatabaseProxy>>();
    targetMap = new HashMap<Long, Integer>();
    replicasMap = new HashMap<String, List<QoSDBCDatabaseProxy>>();
  }

  synchronized public QoSDBCDatabaseProxy getTarget(long proxyId, String dbName) {
    if (!IsValidTenant(dbName)) return null;
    if (!tenantMap.containsKey(proxyId)) {
      OutputMessage.println("[LoadBalancer] ERROR - There is no dbName = "
              + dbName + " monitored. Could not get connection to it.");
      return null;
    }
    List<QoSDBCDatabaseProxy> connectionList = tenantMap.get(proxyId);
    if (connectionList.size() == 1) {
      targetMap.put(proxyId, 0);
      return connectionList.get(0);
    }
    int currentIndex = targetMap.get(proxyId);
    if (currentIndex >= connectionList.size() - 1) {
      currentIndex = 0;
    } else {
      currentIndex++;
    }
    targetMap.put(proxyId, currentIndex); // save the one it'll be using
    //OutputMessage.println("[LoadBalancer] Chosen target: " + currentIndex);
    return connectionList.get(currentIndex);
  }

  synchronized public void addTenant(long proxyId, String dbName, QoSDBCDatabaseProxy conn) {
    if (!IsValidTenant(dbName)) return;
    if (!tenantMap.containsKey(proxyId)) {
      List<QoSDBCDatabaseProxy> connectionList = new ArrayList<QoSDBCDatabaseProxy>();
      connectionList.add(conn);
      targetMap.put(proxyId, 0);
      tenantMap.put(proxyId, connectionList);
      OutputMessage.println("[LoadBalancer] Added Tenant "
              + proxyId);
    }
  }

  synchronized public void addReplica(String dbName, String destinationHost) {
    if (!IsValidTenant(dbName)) return;
    Iterator it = tenantMap.entrySet().iterator();
    ArrayList<QoSDBCDatabaseProxy> connList = new ArrayList<QoSDBCDatabaseProxy>();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry) it.next();
      List<QoSDBCDatabaseProxy> connectionList = (List<QoSDBCDatabaseProxy>) pair.getValue();
      if (connectionList.get(0).getDbName().equals(dbName)) {
        try {
          QoSDBCDatabaseProxy newConn = createNewConn(destinationHost,
                  dbName,
                  connectionList.get(0).getConnection().getAutoCommit());
          connList.add(newConn);
          connectionList.add(newConn);
        } catch (Exception ex) {
          OutputMessage.println("[QoSDBCLoadBalancer::addReplica]: ERROR");
        }
      }
    }
    if (!replicasMap.containsKey(dbName)) {
      List<QoSDBCDatabaseProxy> connectionList = new ArrayList<QoSDBCDatabaseProxy>();
      connectionList.addAll(connList);
      QoSDBCService.consistencyService.addTenantAtHost(dbName, connList.get(0).getVmId());
      replicasMap.put(dbName, connectionList);
    } else {
      List<QoSDBCDatabaseProxy> connectionList = replicasMap.get(dbName);
      connectionList.addAll(connList);
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
    /*if(!IsValidTenant(dbName)) return;
    Iterator it = tenantMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry)it.next();
      List<QoSDBCDatabaseProxy> connectionList = (List<QoSDBCDatabaseProxy>)pair.getValue();
      connectionList.
    }*/
  }


  synchronized public void removeAllReplicas() {
    Iterator it = replicasMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry) it.next();
      List<QoSDBCDatabaseProxy> connectionList = (List<QoSDBCDatabaseProxy>) pair.getValue();
      for (QoSDBCDatabaseProxy conn : connectionList) {
        conn.close();
      }
    }
    replicasMap.clear();
  }

  synchronized public void removeTenant(long proxyId) {
    if (tenantMap.containsKey(proxyId)) {
      tenantMap.get(proxyId).get(0).close();
      tenantMap.remove(proxyId);
      targetMap.remove(proxyId);
      OutputMessage.println("[LoadBalancer] Removed Tenant "
              + proxyId);
      if (tenantMap.isEmpty()) {
        OutputMessage.println("[LoadBalancer] All Tenants removed");
      }
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

  public QoSDBCDatabaseProxy createNewConn(String destinationHost, String databaseName, boolean autoCommit) {
    QoSDBCDatabaseProxy newConn = null;
      newConn = new QoSDBCDatabaseProxy("com.mysql.jdbc.Driver", "jdbc:mysql://" + destinationHost + ":3306/" +
              databaseName,
              databaseName,
              "root",
              "ufc123",
              destinationHost,
              autoCommit);

    return newConn;
  }
}
