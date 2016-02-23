/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.agent;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import qosdbc.commons.OutputMessage;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class InterruptProxy extends Thread {

    private Connection catalogConnection;
    private String vmId;

    public InterruptProxy(Connection catalogConnection, String vmId) {
        this.catalogConnection = catalogConnection;
        this.vmId = vmId;
    }

    @Override
    public void run() {
        try {
            System.out.println("");
            Statement statement = catalogConnection.createStatement();
            OutputMessage.println("Removing active databases references in the qosdbc-catalog...");
            int resultDBActive = statement.executeUpdate("DELETE FROM db_active WHERE vm_id = '" + vmId + "'");
            int resultVMActive = statement.executeUpdate("DELETE FROM vm_active WHERE vm_id = '" + vmId + "'");
            statement.close();
            if (catalogConnection != null) {
                catalogConnection.close();
            }
            OutputMessage.println("Removed all active databases references in the qosdbc-catalog (" + (resultDBActive) +" are removed)");
            OutputMessage.println("Removed all active virtual machines references in the qosdbc-catalog (" + (resultVMActive) +" are removed)");
            OutputMessage.println("The qosdbc-agent was ended");
        } catch (SQLException ex) {
            OutputMessage.println("ERROR: " + ex.getMessage());
        }
    }
}