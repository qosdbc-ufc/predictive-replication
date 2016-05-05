/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import qosdbc.commons.OutputMessage;

/**
 * @author serafim
 */
public class QoSDBCLoadBalancer {
    
    // HashTable Master -> Current Replica(most recent replica to receive a workload)
    private Map<String, String> currentReplicaTable = null;
    
    public QoSDBCLoadBalancer() {
        currentReplicaTable = new ConcurrentHashMap<String, String>();
    }
    
    public String getCurrentVmId(String vmId) {
        if (currentReplicaTable.containsKey(vmId)) {
            return currentReplicaTable.get(vmId);
        }
        return null;
    }
    
    /**
     * 
     * @param vmId 
     */
    public void addMaster(String vmId) {
        if (!currentReplicaTable.containsKey(vmId)) {
            OutputMessage.println("[LoadBalancer]: Added master: " + vmId);
            currentReplicaTable.put(vmId, vmId);
        }
    }
    
    /**
     * 
     * @param vmId
     * @return 
     */
    public String removeMaster(String vmId) {
        OutputMessage.println("[LoadBalancer]: removed master: " + vmId);
        return currentReplicaTable.remove(vmId);
    }
    
    /**
     * 
     * @param catalogConnection
     * @param dbName Name of the database
     * @param vmId VM Id in which the database master is in
     * @return replicaVmId
     */
    public String targetReplica(Connection catalogConnection, String dbName, String vmId) {
        // default target to receive workload is the master
        // OutputMessage.println("[LoadBalancer]: Current: " + currentReplicaTable.get(vmId));
        String targetReplica = vmId;
        try {
            Statement statement = catalogConnection.createStatement();
            // selects all the replicas from the replicas table 
            ResultSet resultSet = statement.executeQuery("SELECT vm_id FROM db_active_replica WHERE master='" + vmId + "/" + dbName + "'");
            while (resultSet.next()) {
                String replicaVmId = resultSet.getString("vm_id");
                // OutputMessage.println("[LoadBalancer]: Queried: " + replicaVmId);
                // the current target replica is the master
                if (currentReplicaTable.get(vmId).equals(vmId)) {
                    targetReplica = replicaVmId;
                    break;
                } else if (currentReplicaTable.get(vmId).equals(replicaVmId)) {
                    if (resultSet.next()) {
                        replicaVmId = resultSet.getString("vm_id");
                        targetReplica = replicaVmId;
                        break;
                    }
                }
            }
            resultSet.close();
            statement.close();
        } catch (SQLException ex) {
            OutputMessage.println("[LoadBalancer]: ERROR on selecting next target for " + vmId + "/" + dbName);
            Logger.getLogger(QoSDBCLoadBalancer.class.getName()).log(Level.SEVERE, null, ex);
        }
        // updates the most recent replica that has processed a workload
        currentReplicaTable.put(vmId, targetReplica);
        // OutputMessage.println("[LoadBalancer]: selected Target ip: " + targetReplica + "/" + dbName);
        return targetReplica;
    }
}
