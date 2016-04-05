/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import qosdbc.commons.OutputMessage;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        OutputMessage.println("### QoSDBC Coordinator v.0.1");
        /* Information by qosdbc-coordinator.properties */
        String fileProperties = System.getProperty("user.dir") + System.getProperty("file.separator") + "qosdbc-coordinator.properties";
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileProperties));
        } catch (FileNotFoundException ex) {
            properties = null;
            qosdbc.commons.OutputMessage.println("The properties file was not found, using information passed by argument");
        } catch (IOException ex) {
            properties = null;
            qosdbc.commons.OutputMessage.println("The properties file was not found, using information passed by argument");
        }

        String qosdbcDbPortParam = null;
        String terminalPortParam = null;
        String catalogHostParam = null;
        String catalogPortParam = null;
        String catalogUserParam = null;
        String catalogPasswordParam = null;
        String logHostParam = null;
        String logPortParam = null;
        String logUserParam = null;
        String logPasswordParam = null;

        if (properties != null) {
            qosdbcDbPortParam = properties.getProperty("qosdbc_db_port");
            if (qosdbcDbPortParam == null || qosdbcDbPortParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of qosdbc_db_port parameter");
                System.exit(0);
            }
            terminalPortParam = properties.getProperty("terminal_port");
            if (terminalPortParam == null || terminalPortParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of terminal_port parameter");
                System.exit(0);
            }
            catalogHostParam = properties.getProperty("catalog_host");
            if (catalogHostParam == null || catalogHostParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of catalog_host parameter");
                System.exit(0);
            }
            catalogPortParam = properties.getProperty("catalog_port");
            if (catalogPortParam == null || catalogPortParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of catalog_port parameter");
                System.exit(0);
            }
            catalogUserParam = properties.getProperty("catalog_user");
            if (catalogUserParam == null || catalogUserParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of catalog_user parameter");
                System.exit(0);
            }
            catalogPasswordParam = properties.getProperty("catalog_password");
            if (catalogPasswordParam == null || catalogPasswordParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of catalog_password parameter");
                System.exit(0);
            }

            logHostParam = properties.getProperty("log_host");
            if (logHostParam == null || logHostParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of log_host parameter");
                System.exit(0);
            }
            logPortParam = properties.getProperty("log_port");
            if (logPortParam == null || logPortParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of log_port parameter");
                System.exit(0);
            }
            logUserParam = properties.getProperty("log_user");
            if (logUserParam == null || logUserParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of log_user parameter");
                System.exit(0);
            }
            logPasswordParam = properties.getProperty("log_password");
            if (logPasswordParam == null || logPasswordParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of log_password parameter");
                System.exit(0);
            }
        }

        if (properties == null && (args == null || args.length != 10)) {
            OutputMessage.println("Command Sintax......: java -jar qosdbc-coordinator.jar <qosdbc_db_port> <terminal_port> <catalog_host> <catalog_storage_port> <catalog_user> <catalog_password> <log_host> <log_port> <log_user> <log_password>");
            OutputMessage.println("Command Example.....: java -jar qosdbc-coordinator.jar 7777 7778 qosdbc.catalog 5432 catalog_user catalog_password qosdbc.log 5432 log_user log_password");
        } else {
            if (properties == null) {
                qosdbcDbPortParam = args[0];
                terminalPortParam = args[1];
                catalogHostParam = args[2];
                catalogPortParam = args[3];
                catalogUserParam = args[4];
                catalogPasswordParam = args[5];
                logHostParam = args[6];
                logPortParam = args[7];
                logUserParam = args[8];
                logPasswordParam = args[9];
            }

            OutputMessage.println("Parameters: ");
            OutputMessage.println("Port.....................: " + qosdbcDbPortParam);
            OutputMessage.println("Terminal Port............: " + terminalPortParam);
            OutputMessage.println("Catalog Host.............: " + catalogHostParam);
            OutputMessage.println("Catalog Port.............: " + catalogPortParam);
            OutputMessage.println("Catalog User.............: " + catalogUserParam);
            OutputMessage.println("Catalog Password.........: " + catalogPasswordParam);
            OutputMessage.println("Log Host.................: " + logHostParam);
            OutputMessage.println("Log Port.................: " + logPortParam);
            OutputMessage.println("Log User.................: " + logUserParam);
            OutputMessage.println("Log Password.............: " + logPasswordParam);

            int qosdbcDbPort = -1;
            try {
                qosdbcDbPort = Integer.parseInt(qosdbcDbPortParam);
            } catch (Exception ex) {
                OutputMessage.println("ERROR: " + qosdbcDbPortParam + " is not a port valid");
                System.exit(0);
            }

            int terminalPort = -1;
            try {
                terminalPort = Integer.parseInt(terminalPortParam);
            } catch (Exception ex) {
                OutputMessage.println("ERROR: " + terminalPortParam + " is not a port valid");
                System.exit(0);
            }

            int catalogStoragePort = -1;
            try {
                catalogStoragePort = Integer.parseInt(catalogPortParam);
            } catch (Exception ex) {
                OutputMessage.println("ERROR: " + catalogPortParam + " is not a port valid");
                System.exit(0);
            }

            int logStoragePort = -1;
            try {
                logStoragePort = Integer.parseInt(logPortParam);
            } catch (Exception ex) {
                OutputMessage.println("ERROR: " + logPortParam + " is not a port valid");
                System.exit(0);
            }

            // Creates the connection with the Catalog's Service
            Connection catalogConnection = null;
            try {
                Class.forName("org.postgresql.Driver");
                catalogConnection = DriverManager.getConnection("jdbc:postgresql://" + catalogHostParam + ":" + catalogStoragePort + "/qosdbc-catalog", catalogUserParam, catalogPasswordParam);
                qosdbc.commons.OutputMessage.println("qosdbc-coordinator: " + "connected to Catalog Service");
            } catch (SQLException ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
                System.exit(0);
            } catch (ClassNotFoundException ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
                System.exit(0);
            }

            // Creates the connection with the Log's Service
            Connection logConnection = null;
            try {
                Class.forName("org.postgresql.Driver");
                logConnection = DriverManager.getConnection("jdbc:postgresql://" + logHostParam + ":" + logStoragePort + "/qosdbc-log", logUserParam, logPasswordParam);
                qosdbc.commons.OutputMessage.println("qosdbc-coordinator: " + "connected to Log Service");
            } catch (SQLException ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
                System.exit(0);
            } catch (ClassNotFoundException ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
                System.exit(0);
            }

            //QoSDBCBackupOrganizer qosdbcBackupOrganizer = new QoSDBCBackupOrganizer();
            //qosdbcBackupOrganizer.start();

            /*
            try {
                Provider provider = new ProviderAmazonImpl();
                provider.connect();
                List<VirtualMachine> virtualMachines = provider.getVirtualMachineList("leonardo");
                System.out.println("Show all Amazon Virtual Machine by Owner 'leonardo'");
                for (VirtualMachine vm : virtualMachines) {
                    qosdbc.commons.OutputMessage.println(vm.toString());
                }
                provider.disconnect();
            } catch (ProviderException ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
                System.exit(0);
            }
            */ 
                        
            QoSDBCService qosdbcService = new QoSDBCService(qosdbcDbPort, catalogConnection, logConnection);
            qosdbcService.start();

            QoSDBCTerminalServer terminalServer = new QoSDBCTerminalServer(terminalPort, catalogConnection, logConnection, qosdbcService);
            terminalServer.start();
        }
    }
}
