/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.agent;

import qosdbc.commons.ShellCommand;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import qosdbc.commons.Database;
import qosdbc.commons.DatabaseSystem;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        qosdbc.commons.OutputMessage.println("### QoSDBC Agent v.0.1");
        /* Information by qosdbc-agent.properties */
        String fileProperties = System.getProperty("user.dir") + System.getProperty("file.separator") + "qosdbc-agent.properties";
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

        String localIPAddress = null;
        String localPortParam = null;
        String dbmsConfigurationFileParam = null;
        String timeParam = null;

        String catalogHostParam = null;
        String catalogPortParam = null;
        String catalogUserParam = null;
        String catalogPasswordParam = null;

        String logHostParam = null;
        String logPortParam = null;
        String logUserParam = null;
        String logPasswordParam = null;

        String startAgentParam = null;
        String startMonitorParam = null;

        if (properties != null) {
            localIPAddress = properties.getProperty("local_ip_address");
            if (localIPAddress == null || localIPAddress.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of local_ip_address parameter");
                System.exit(0);
            }
            localPortParam = properties.getProperty("local_port");
            if (localPortParam == null || localPortParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of local_port parameter");
                System.exit(0);
            }
            dbmsConfigurationFileParam = properties.getProperty("dbms_configuration_file");
            if (dbmsConfigurationFileParam == null || dbmsConfigurationFileParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of dbms_configuration_file parameter");
                System.exit(0);
            }
            timeParam = properties.getProperty("time_delay");
            if (timeParam == null || timeParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of time_delay parameter");
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

            startAgentParam = properties.getProperty("agent");
            if (startAgentParam == null || startAgentParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of agent parameter");
                System.exit(0);
            } else {
                startAgentParam = "--agent=" + startAgentParam;
            }
            startMonitorParam = properties.getProperty("monitor");
            if (startMonitorParam == null || startMonitorParam.trim().length() == 0) {
                qosdbc.commons.OutputMessage.println("ERROR: Error in the value of monitor parameter");
                System.exit(0);
            } else {
                startMonitorParam = "--monitor=" + startMonitorParam;
            }
        }

        if (properties == null && (args == null || args.length != 14)) {
            qosdbc.commons.OutputMessage.println("Command Sintax..: java -jar qosdbc-agent.jar <local_ip_address> <local_port> <dbms_configuration_file> <time_delay> <catalog_host> <catalog_port> <catalog_user> <catalog_password> <log_host> <log_port> <log_user> <log_password> --agent=true --monitor=true");
            qosdbc.commons.OutputMessage.println("Command Example.: java -jar qosdbc-agent.jar 192.168.0.100 7778 /home/leoomoreira/dbms.xml 1000 qosdbc.catalog 5432 catalog_user catalog_password qosdbc.log 5432 log_user log_password --agent=true --monitor=true");
        } else {
            if (properties == null) {
                localIPAddress = args[0];
                localPortParam = args[1];
                dbmsConfigurationFileParam = args[2];
                timeParam = args[3];

                catalogHostParam = args[4];
                catalogPortParam = args[5];
                catalogUserParam = args[6];
                catalogPasswordParam = args[7];

                logHostParam = args[8];
                logPortParam = args[9];
                logUserParam = args[10];
                logPasswordParam = args[11];

                startAgentParam = args[12];
                startMonitorParam = args[13];
            }

            qosdbc.commons.OutputMessage.println("Parameters: ");
            qosdbc.commons.OutputMessage.println("Local IP Address.........: " + localIPAddress);
            qosdbc.commons.OutputMessage.println("Local Port...............: " + localPortParam);
            qosdbc.commons.OutputMessage.println("DBMSs Configuration File.: " + dbmsConfigurationFileParam);
            qosdbc.commons.OutputMessage.println("Time (millis)............: " + timeParam);

            qosdbc.commons.OutputMessage.println("Catalog Host.............: " + catalogHostParam);
            qosdbc.commons.OutputMessage.println("Catalog Port.............: " + catalogPortParam);
            qosdbc.commons.OutputMessage.println("Catalog User.............: " + catalogUserParam);
            qosdbc.commons.OutputMessage.println("Catalog Password.........: " + catalogPasswordParam);

            qosdbc.commons.OutputMessage.println("Log Host.................: " + catalogHostParam);
            qosdbc.commons.OutputMessage.println("Log Port.................: " + catalogPortParam);
            qosdbc.commons.OutputMessage.println("Log User.................: " + catalogUserParam);
            qosdbc.commons.OutputMessage.println("Log Password.............: " + catalogPasswordParam);

            boolean startAgent = false;
            if (startAgentParam != null && startAgentParam.equals("--agent=true")) {
                startAgent = true;
            }
            boolean startMonitor = false;
            if (startMonitorParam != null && startMonitorParam.equals("--monitor=true")) {
                startMonitor = true;
            }
            qosdbc.commons.OutputMessage.println("Start QoSDBCAgent........: " + startAgent);
            qosdbc.commons.OutputMessage.println("Start QoSDBCMonitor......: " + startMonitor);

            // Define the VM Id
            String vmId = "";
            qosdbc.commons.OutputMessage.println("Validating the local IP address specified...");
            try {
                if (InetAddress.getLocalHost().getHostAddress().equals(localIPAddress)) {
                    vmId = localIPAddress;
                } else {
                    boolean isValid = false;
                    Enumeration<NetworkInterface> n = NetworkInterface.getNetworkInterfaces();
                    for (; n.hasMoreElements();) {
                        NetworkInterface e = n.nextElement();
                        Enumeration<InetAddress> a = e.getInetAddresses();
                        while (a.hasMoreElements()) {
                            InetAddress addr = a.nextElement();
                            if (addr.getHostAddress().equals(localIPAddress)) {
                                vmId = localIPAddress;
                                qosdbc.commons.OutputMessage.println("The local IP address specified is a valid and present in the interface " + e.getName());
                                isValid = true;
                                break;
                            }
                        }
                    }
                    if (!isValid) {
                        qosdbc.commons.OutputMessage.println("ERROR: " + localIPAddress + " is not a IP address valid");
                        System.exit(0);
                    }
                }
            } catch (Exception ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + localIPAddress + " is not a IP address valid");
                System.exit(0);
            }
            qosdbc.commons.OutputMessage.println("Virtual Machine ID.......: " + vmId);

            int localPort = -1;
            try {
                localPort = Integer.parseInt(localPortParam);
            } catch (Exception ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + localPortParam + " is not a port valid");
                System.exit(0);
            }

            String dbmsConfigurationFile = dbmsConfigurationFileParam;
            List<DatabaseSystem> databaseSystems = Main.getDatabaseSystems(dbmsConfigurationFile);
            if (databaseSystems.size() <= 0) {
                qosdbc.commons.OutputMessage.println("ERROR: " + "There are no DBMSs to be monitored");
                System.exit(0);
            } else {
                for (DatabaseSystem databaseSystem : databaseSystems) {
                    qosdbc.commons.OutputMessage.println("Found " + databaseSystem.toString() + " User: " + databaseSystem.getUser() + " Password: " + databaseSystem.getPassword());
                }
            }

            long time = -1;
            try {
                time = Long.parseLong(timeParam);
            } catch (Exception ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + timeParam + " is not a number valid");
                System.exit(0);
            }

            String catalogHost = catalogHostParam;

            int catalogPort = -1;
            try {
                catalogPort = Integer.parseInt(catalogPortParam);
            } catch (Exception ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + catalogPortParam + " is not a port valid");
                System.exit(0);
            }

            String logHost = logHostParam;

            int logPort = -1;
            try {
                logPort = Integer.parseInt(logPortParam);
            } catch (Exception ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + logPortParam + " is not a port valid");
                System.exit(0);
            }

            // Creates the connection with the Catalog's Service
            Connection catalogConnection = null;
            try {
                Class.forName("org.postgresql.Driver");
                catalogConnection = DriverManager.getConnection("jdbc:postgresql://" + catalogHost + ":" + catalogPort + "/qosdbc-catalog", catalogUserParam, catalogPasswordParam);
                qosdbc.commons.OutputMessage.println("qosdbc-agent: " + "connected to Catalog Service");
            } catch (SQLException ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
                System.exit(0);
            } catch (ClassNotFoundException ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
                System.exit(0);
            }

            Connection logConnection = null;
            try {
                Class.forName("org.postgresql.Driver");
                logConnection = DriverManager.getConnection("jdbc:postgresql://" + logHost + ":" + logPort + "/qosdbc-log", logUserParam, logPasswordParam);
                qosdbc.commons.OutputMessage.println("qosdbc-agent: " + "connected to Log Service");
            } catch (SQLException ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
                System.exit(0);
            } catch (ClassNotFoundException ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
                System.exit(0);
            }

            // Testing all DBMSs and databases
            if (databaseSystems == null || databaseSystems.isEmpty()) {
                qosdbc.commons.OutputMessage.println("ERROR: None DBMS was found ");
                System.exit(0);
            }
            try {
                Statement statement = catalogConnection.createStatement();
                for (DatabaseSystem databaseSystem : databaseSystems) {
                    if (!ShellCommand.checkDBMSActive(databaseSystem.getType())) {
                        qosdbc.commons.OutputMessage.println("ERROR: " + databaseSystem.toString() + " is not installed or running");
                        System.exit(0);
                    } else {
                        qosdbc.commons.OutputMessage.println("qosdbc-agent: " + databaseSystem.toString() + " is installed and running");
                        switch (databaseSystem.getType()) {
                            case qosdbc.commons.DatabaseSystem.TYPE_MYSQL: {
                                List<Database> mysqlDatabases = ShellCommand.getDatabases(databaseSystem.getType(), databaseSystem.getUser(), databaseSystem.getPassword());
                                try {
                                    for (Database database : mysqlDatabases) {
                                        String dbName = database.getName();
                                        String schemaDefinition = ShellCommand.dumpDatabase(database, databaseSystem.getUser(), databaseSystem.getPassword());
                                        if (schemaDefinition != null) {
                                            schemaDefinition = schemaDefinition.replaceAll("[\']", "''");
                                        }
                                        int result = statement.executeUpdate("INSERT INTO db_active (\"time\", vm_id, db_name, dbms_type, schema_definition, dbms_user, dbms_password, dbms_port) VALUES (now(), '" + vmId + "', '" + dbName + "', " + databaseSystem.getType() + ", '" + schemaDefinition + "', 'root', 'ufc123', 3306)");
                                        qosdbc.commons.OutputMessage.println("qosdbc-agent: [db_active] Database " + dbName + " was inserted (" + (result == 1) + ")");
                                    }
                                } catch (SQLException ex) {
                                    throw ex;
                                }
                                break;
                            }
                            case qosdbc.commons.DatabaseSystem.TYPE_POSTGRES: {
                                List<Database> postgresDatabases = ShellCommand.getDatabases(databaseSystem.getType(), databaseSystem.getUser(), databaseSystem.getPassword());
                                try {
                                    for (Database database : postgresDatabases) {
                                        String dbName = database.getName();
                                        /* Call the dump statement */
                                        String schemaDefinition = ShellCommand.dumpDatabase(database, databaseSystem.getUser(), databaseSystem.getPassword());
                                        if (schemaDefinition != null) {
                                            schemaDefinition = schemaDefinition.replaceAll("[\']", "''");
                                        }
                                        int result = statement.executeUpdate("INSERT INTO db_active (\"time\", vm_id, db_name, dbms_type, schema_definition, dbms_user, dbms_password, dbms_port) VALUES (now(), '" + vmId + "', '" + dbName + "', " + databaseSystem.getType() + ", '" + schemaDefinition + "', 'postgres', 'ufc123', 5432)");
                                        qosdbc.commons.OutputMessage.println("qosdbc-agent: [db_active] Database " + dbName + " was inserted (" + (result == 1) + ")");
                                    }
                                } catch (SQLException ex) {
                                    throw ex;
                                }
                                break;
                            }
                        }

                    }
                }
                statement.close();
            } catch (SQLException ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
                System.exit(0);
            }
            // Insert VM in vm_active table
            try {
                Statement statement = catalogConnection.createStatement();
                long memTotal = ShellCommand.getMemoryTotal();
                long diskTotal = ShellCommand.getDiskTotal();
                int result = statement.executeUpdate("INSERT INTO vm_active (\"time\", vm_id, mem_total, disk_total, agent_port) VALUES (now(), '" + vmId + "', " + memTotal + ", " + diskTotal + ", " + localPort + ")");
                qosdbc.commons.OutputMessage.println("qosdbc-agent: [vm_active] VM " + vmId + " was inserted (" + (result == 1) + ")");
                statement.close();
            } catch (SQLException ex) {
                qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
                System.exit(0);
            }

            // Init the QoSDBCMonitor
            if (startMonitor) {
                try {
                    QoSDBCMonitor qosdbcMonitor = new QoSDBCMonitor(vmId, time, catalogConnection, databaseSystems);
                    qosdbcMonitor.start();
                } catch (SQLException ex) {
                    qosdbc.commons.OutputMessage.println("ERROR: " + ex.getMessage());
                    System.exit(0);
                }
            }

            // Init the QoSDBCAgent that receives commands of the QoSDBCServer
            if (startAgent) {
                QoSDBCAgent qosdbcAgent = new QoSDBCAgent(vmId, localPort, time, catalogConnection, logConnection, databaseSystems);
                qosdbcAgent.start();
            }

            Runtime.getRuntime().addShutdownHook(new InterruptProxy(catalogConnection, vmId));
        }
    }

    public static List<DatabaseSystem> getDatabaseSystems(String filePath) {
        List<DatabaseSystem> result = new ArrayList<DatabaseSystem>();
        SAXBuilder builder = new SAXBuilder();
        Document document = null;
        try {
            document = builder.build(new File(filePath));
            List dbmsList = document.getRootElement().getChildren();
            for (int i = 0; dbmsList != null && i < dbmsList.size(); i++) {
                Element databaseSystemElement = (Element) dbmsList.get(i);
                DatabaseSystem databaseSystem = new DatabaseSystem();
                databaseSystem.setType(Integer.parseInt(databaseSystemElement.getChildText("type")));
                databaseSystem.setUser(databaseSystemElement.getChildText("user"));
                databaseSystem.setPassword(databaseSystemElement.getChildText("password"));
                result.add(databaseSystem);
            }
        } catch (IOException ex) {
            qosdbc.commons.OutputMessage.println("ERROR: QoSDBCAgent: " + ex.getMessage());
            System.exit(0);
        } catch (JDOMException ex) {
            qosdbc.commons.OutputMessage.println("ERROR: QoSDBCAgent: " + ex.getMessage());
            System.exit(0);
        }
        return result;
    }
}