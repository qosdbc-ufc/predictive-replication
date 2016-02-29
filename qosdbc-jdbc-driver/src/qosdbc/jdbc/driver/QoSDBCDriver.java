/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.jdbc.driver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;
import qosdbc.commons.OutputMessage;

/**
 *
 * @author Leonardo Oliveira Moreira
 *
 */
public class QoSDBCDriver implements Driver {

    static QoSDBCDriver singleton;
    static Properties properties;

    static {
        singleton = new QoSDBCDriver();
        try {
            DriverManager.registerDriver(singleton);
        } catch (SQLException e) {
            throw new RuntimeException("Unable to register qosdbc driver!");
        }
        File outputFile = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + "qosdbc-jdbc-driver.properties");
        if (outputFile != null && outputFile.exists()) {
            try {
                properties = new Properties();
                properties.load(new FileInputStream(outputFile));
            } catch (IOException ex) {
            }
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (properties != null) {
            Enumeration enumeration = properties.keys();
            while (enumeration.hasMoreElements()) {
                String key = (String) enumeration.nextElement();
                if (key != null) {
                    info.setProperty(key, properties.getProperty(key));
                }
            }
        }
        if (!url.startsWith("jdbc:qosdbc")) {
            return null;
        }
        String[] split = url.split("//");
        String[] s = split[1].split(":");
        String host = s[0];
        int port = 7777;
        if (s.length > 1) {
            port = Integer.parseInt(s[1].split("/")[0]);
        }
        info = new Properties(info);
        info.setProperty("databaseName", s[1].split("/")[1]);
        OutputMessage.println("URL: " + url);
        return new QoSDBCConnection(host, port, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith("jdbc:qosdbc:");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}