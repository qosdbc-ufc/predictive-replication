/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import qosdbc.commons.OutputMessage;


/**
 * @author serafim
 */
public class QoSDBCLoadBalancer {

  // proxyId => QoSDBCDatabaseProxy
  private static Map<Long, QoSDBCDatabaseProxy> tenantMap = null;

  // QoSDBCDatabaseProxy ID => hosts
  private static HashMap<Long, List<String>> hostsMap = null;


  public QoSDBCLoadBalancer() {
    tenantMap = new ConcurrentHashMap<Long, QoSDBCDatabaseProxy>();
    hostsMap = new HashMap<Long, List<String>>();

  }

  public QoSDBCDatabaseProxy getTarget(long proxyId, String dbName) {
    //if (!IsValidTenant(dbName)) return null;

      if (!tenantMap.containsKey(proxyId)) {
        OutputMessage.println("[LoadBalancer] ERROR - There is no dbName = "
                + dbName + " monitored. Could not get connection to it.");

        return null;
      }
      QoSDBCDatabaseProxy proxy = tenantMap.get(proxyId);

    return proxy;
  }

  synchronized public void addTenant(long proxyId, String dbName, QoSDBCDatabaseProxy conn) {
    if (!IsValidTenant(dbName)) return;
    if (!tenantMap.containsKey(proxyId)) {
      tenantMap.put(proxyId, conn);
      List<String> hostsList = new ArrayList<String>();
      hostsList.add(conn.getVmId());
      hostsMap.put(conn.getId(), hostsList);
      OutputMessage.println("[LoadBalancer] Added Tenant " + proxyId);
    }
  }

  synchronized public void addReplica(String dbName, String destinationHost) {
    if (!IsValidTenant(dbName)) return;
    Iterator it = tenantMap.entrySet().iterator();
    ArrayList<QoSDBCDatabaseProxy> connList = new ArrayList<QoSDBCDatabaseProxy>();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry) it.next();
      QoSDBCDatabaseProxy proxyConnection = (QoSDBCDatabaseProxy) pair.getValue();
      if (proxyConnection.getDbName().equals(dbName)) {
        try {
          hostsMap.get(proxyConnection.getId()).add(destinationHost);
          QoSDBCDatabaseProxy newConn = createNewBalancedConn(proxyConnection.getId(),
                  dbName,
                  proxyConnection.getConnection().getAutoCommit());

            tenantMap.put(proxyConnection.getId(), newConn);

        } catch (Exception ex) {
          OutputMessage.println("[QoSDBCLoadBalancer::addReplica]: ERROR");
        }
      }
    }
    OutputMessage.println("[QoSDBCLoadBalancer::addReplica]: SUCCESS");
  }

  /*
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
  */

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
    Iterator it = hostsMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry) it.next();
      List<QoSDBCDatabaseProxy> connectionList = (List<QoSDBCDatabaseProxy>) pair.getValue();
      for (QoSDBCDatabaseProxy conn : connectionList) {
        conn.close();
      }
    }
    hostsMap.clear();
  }

  synchronized public void removeTenant(long proxyId) {
    if (tenantMap.containsKey(proxyId)) {

        tenantMap.get(proxyId).close();
        tenantMap.remove(proxyId);
        OutputMessage.println("[LoadBalancer] Removed Tenant "
                + proxyId);
        if (tenantMap.isEmpty()) {
          OutputMessage.println("[LoadBalancer] All Tenants removed");
        }

    }
  }
/*
  synchronized public boolean isAutoCommit(String dbName) {
    if (tenantMap.containsKey(dbName)) {
      try {
        return tenantMap.get(dbName).getConnection().getAutoCommit();
      } catch (SQLException e) {
        OutputMessage.println("[LoadBalancer] ERROR - While getting autoCommit from  "
                + dbName);
        e.printStackTrace();
      }
    }
    return true;
  }
*/
  synchronized private boolean IsValidTenant(String dbName) {
    return !dbName.equals("information_schema")
            && !dbName.equals("mysql")
            && !dbName.equals("performance_schema");
  }

  public QoSDBCDatabaseProxy createNewBalancedConn(long proxyConnectionId, String databaseName, boolean autoCommit) {
    QoSDBCDatabaseProxy newConn = null;
    String hosts = "jdbc:mysql:loadbalance://";
    for(int i=0; i<hostsMap.get(proxyConnectionId).size(); i++) {
      hosts = hostsMap.get(proxyConnectionId).get(i) + ":3306";
      if (i<hostsMap.get(proxyConnectionId).size()-1) {
        hosts += ",";
      }
    }
      newConn = new QoSDBCDatabaseProxy("com.mysql.jdbc.Driver", hosts + "/" + databaseName + "?loadBalanceBlacklistTimeout=5000&loadBalanceStrategy=bestResponseTime",
              databaseName,
              "root",
              "ufc123",
              tenantMap.get(proxyConnectionId).getVmId(),
              autoCommit);

    return newConn;
  }
}
