/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Leonardo Oliveira Moreira
 *
 * Class used to interact with operating system commands
 *
 * OSs Supported: - Linux (Total) - Windows (Partial) DBMSs Supported: - MySQL
 * (Total) - PostgreSQL (Total)
 */
public final class ShellCommand {

    public static final int LINUX = 1;
    public static final int WINDOWS = 2;

    /**
     * Prevents an object be created (Static Class)
     */
    private ShellCommand() {
    }

    /**
     * Method used to check what operating system is installed in this virtual
     * machine
     *
     * @return - A constant that indicates the operating system
     */
    public static int getOperationSystem() {
        String operationalSystem = System.getProperty("os.name");
        if (operationalSystem.equalsIgnoreCase(("linux"))) {
            return LINUX;
        } else {
            return WINDOWS;
        }
    }

    /**
     * Method used to get the percentage of the CPU free in your operation
     * system
     *
     * @return
     */
    public static double getCPUFreePercentage() {
        double result = 0;
        ProcessBuilder process = new ProcessBuilder(new String[]{"bash", "-c", "vmstat | awk '{print $15}' | tail -1"});
        try {
            Process p = process.start();
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String string = "";
            int l = 0;
            while ((string = stdInput.readLine()) != null) {
                if (l == 0 && string != null) {
                    return Double.parseDouble(string);
                }
                l++;
            }
        } catch (Exception ex) {
            return result;
        }
        return result;
    }

    /**
     * Method used to get the total memory avaliable in your operation system
     *
     * @return
     */
    public static long getMemoryTotal() {
        long result = 0;
        ProcessBuilder process = new ProcessBuilder(new String[]{"bash", "-c", "free -m | grep Mem | awk '{print $2}'"});
        try {
            Process p = process.start();
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String string = "";
            int l = 0;
            while ((string = stdInput.readLine()) != null) {
                if (l == 0 && string != null) {
                    return Long.parseLong(string);
                }
                l++;
            }
        } catch (Exception ex) {
            return result;
        }
        return result;
    }

    /**
     * Method used to get the free memory avaliable in your operation system
     *
     * @return
     */
    public static long getMemoryFree() {
        long result = 0;
        ProcessBuilder process = new ProcessBuilder(new String[]{"bash", "-c", "free -m | grep Mem | awk '{print $4}'"});
        try {
            Process p = process.start();
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String string = "";
            int l = 0;
            while ((string = stdInput.readLine()) != null) {
                if (l == 0 && string != null) {
                    return Long.parseLong(string);
                }
                l++;
            }
        } catch (Exception ex) {
            return result;
        }
        return result;
    }

    /**
     * Method used to get the total storage disk avaliable in your operation
     * system
     *
     * @return
     */
    public static long getDiskTotal() {
        long result = 0;
        ProcessBuilder process = new ProcessBuilder(new String[]{"bash", "-c", "df -v . | awk '{print $2}' | tail -1"});
        try {
            Process p = process.start();
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String string = "";
            int l = 0;
            while ((string = stdInput.readLine()) != null) {
                if (l == 0 && string != null) {
                    return Long.parseLong(string);
                }
                l++;
            }
        } catch (Exception ex) {
            return result;
        }
        return result;
    }

    /**
     * Method used to get the free storage disk avaliable in your operation
     * system
     *
     * @return
     */
    public static long getDiskFree() {
        long result = 0;
        ProcessBuilder process = new ProcessBuilder(new String[]{"bash", "-c", "df -v . | awk '{print $4}' | tail -1"});
        try {
            Process p = process.start();
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String string = "";
            int l = 0;
            while ((string = stdInput.readLine()) != null) {
                if (l == 0 && string != null) {
                    return Long.parseLong(string);
                }
                l++;
            }
        } catch (Exception ex) {
            return result;
        }
        return result;
    }

    /**
     * Method used to check if the DBMS informed by parameter is active
     *
     * @param databaseSystemType - DBMS's Type
     * @return - DBMS's state (active or not)
     */
    public static boolean checkDBMSActive(int databaseSystemType) {
        switch (databaseSystemType) {
            case DatabaseSystem.TYPE_MYSQL: {
                ProcessBuilder pbMysql = new ProcessBuilder(new String[]{"bash", "-c", "mysql --version"});
                try {
                    Process p = pbMysql.start();
                    BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                    String mysql = "";
                    int l = 0;
                    while ((mysql = stdInput.readLine()) != null) {
                        if (l == 0 && mysql != null && mysql.startsWith("mysql")) {
                            return true;
                        }
                        l++;
                    }
                } catch (IOException ex) {
                    return false;
                }
                break;
            }
            case DatabaseSystem.TYPE_POSTGRES: {
                ProcessBuilder pbPsql = new ProcessBuilder(new String[]{"bash", "-c", "psql --version"});
                try {
                    Process p = pbPsql.start();
                    BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                    String psql = "";
                    int l = 0;
                    while ((psql = stdInput.readLine()) != null) {
                        if (l == 0 && psql != null && psql.startsWith("psql")) {
                            return true;
                        }
                        l++;
                    }
                } catch (IOException ex) {
                    return false;
                }
                break;
            }
        }
        return false;
    }

    /**
     * Method used to check which DBMSs are running on this virtual machine
     *
     * @return - List of DBMSs
     */
    public static List<DatabaseSystem> getDatabaseSystems() {
        List<DatabaseSystem> result = new ArrayList<DatabaseSystem>();
        if (getOperationSystem() == LINUX) {
            if (checkDBMSActive(DatabaseSystem.TYPE_POSTGRES)) {
                DatabaseSystem ds = new DatabaseSystem();
                ds.setType(DatabaseSystem.TYPE_POSTGRES);
                result.add(ds);
            }
            if (checkDBMSActive(DatabaseSystem.TYPE_MYSQL)) {
                DatabaseSystem ds = new DatabaseSystem();
                ds.setType(DatabaseSystem.TYPE_MYSQL);
                result.add(ds);
            }
        }
        return result;
    }

    public static List<Database> getDatabases(int databaseSystemType, String username, String password) {
        List<Database> result = new ArrayList<Database>();
        if (getOperationSystem() == LINUX) {
            switch (databaseSystemType) {
                case DatabaseSystem.TYPE_POSTGRES: {
                    try {
                        String s = "";
                        ProcessBuilder pb = new ProcessBuilder(new String[]{"psql", "-h", "localhost", "-U", username, "-l"});
                        Map<String, String> env = pb.environment();
                        env.put("PGPASSWORD", password);
                        Process p = pb.start();
                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        s = "";
                        int c = 0;
                        while ((s = stdInput.readLine()) != null) {
                            String[] b = s.split("[|]");
                            if (b != null && b.length == 6 && b[0].trim().length() > 0 && c > 1) {
                                Database database = new Database(b[0].trim(), DatabaseSystem.TYPE_POSTGRES);
                                result.add(database);
                            }
                            c++;
                        }
                        while ((s = stdError.readLine()) != null) {
                            result.clear();
                        }
                    } catch (IOException e) {
                        result.clear();
                    }
                    break;
                }
                case DatabaseSystem.TYPE_MYSQL: {
                    try {
                        String s = "";
                        ProcessBuilder pb = new ProcessBuilder(new String[]{"mysqlshow", "-u", username, "--password=" + password});
                        Process p = pb.start();
                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        s = "";
                        int c = 0;
                        while ((s = stdInput.readLine()) != null) {
                            if (s.trim().length() > 2 && c > 2) {
                                Database database = new Database(s.trim().substring(1, s.trim().length() - 1).trim(), DatabaseSystem.TYPE_MYSQL);
                                result.add(database);
                            }
                            c++;
                        }
                        if (result.size() > 0) {
                            result.remove(result.size() - 1);
                        }
                        while ((s = stdError.readLine()) != null) {
                            result.clear();
                        }
                    } catch (IOException e) {
                        result.clear();
                    }
                    break;
                }
            }
        }
        return result;
    }

    public static boolean dropDatabase(Database database, String username, String password) {
        boolean success = false;
        if (getOperationSystem() == LINUX) {
            success = true;
            switch (database.getType()) {
                case DatabaseSystem.TYPE_POSTGRES: {
                    try {
                        String s = null;
                        ProcessBuilder pb = new ProcessBuilder(new String[]{"dropdb", "-h", "localhost", "-U", username, database.getName()});
                        Map<String, String> env = pb.environment();
                        env.put("PGPASSWORD", password);
                        Process p = pb.start();
                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        s = "";
                        while ((s = stdInput.readLine()) != null) {
                        }
                        while ((s = stdError.readLine()) != null) {
                            success = false;
                        }
                    } catch (IOException e) {
                        success = false;
                    }
                    break;
                }
                case DatabaseSystem.TYPE_MYSQL: {
                    try {
                        String s = null;
                        ProcessBuilder pb = new ProcessBuilder(new String[]{"mysqladmin", "-u", username, "-p", "drop", database.getName(), "--password=" + password, "-f"});
                        Process p = pb.start();
                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        s = "";
                        while ((s = stdInput.readLine()) != null) {
                        }
                        while ((s = stdError.readLine()) != null) {
                            success = false;
                        }
                    } catch (IOException e) {
                        success = false;
                    }
                    break;
                }
            }
        }
        return success;
    }

    public static boolean createDatabase(Database database, String username, String password) {
        boolean success = false;
        if (getOperationSystem() == LINUX) {
            success = true;
            switch (database.getType()) {
                case DatabaseSystem.TYPE_POSTGRES: {
                    try {
                        String s = null;
                        ProcessBuilder pb = new ProcessBuilder(new String[]{"createdb", "-h", "localhost", "-U", username, database.getName()});
                        Map<String, String> env = pb.environment();
                        env.put("PGPASSWORD", password);
                        Process p = pb.start();
                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        s = "";
                        while ((s = stdInput.readLine()) != null) {
                        }
                        while ((s = stdError.readLine()) != null) {
                            success = false;
                        }
                    } catch (IOException e) {
                        success = false;
                    }
                    break;
                }
                case DatabaseSystem.TYPE_MYSQL: {
                    try {
                        String s = null;
                        ProcessBuilder pb = new ProcessBuilder(new String[]{"mysqladmin", "-u", username, "-p", "create", database.getName(), "--password=" + password});
                        Process p = pb.start();
                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        s = "";
                        while ((s = stdInput.readLine()) != null) {
                        }
                        while ((s = stdError.readLine()) != null) {
                            success = false;
                        }
                    } catch (IOException e) {
                        success = false;
                    }
                    break;
                }
            }
        }
        return success;
    }

    public static String dumpDatabase(Database database, String username, String password) {
        String result = "";
        boolean success = false;
        if (getOperationSystem() == LINUX) {
            success = true;
            switch (database.getType()) {
                case DatabaseSystem.TYPE_POSTGRES: {
                    try {
                        String s = null;
                        ProcessBuilder pb = new ProcessBuilder(new String[]{"pg_dump", "-h", "localhost", "-U", username, "-s", "-F", "p", database.getName()});
                        Map<String, String> env = pb.environment();
                        env.put("PGPASSWORD", password);
                        Process p = pb.start();
                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        s = "";
                        while ((s = stdInput.readLine()) != null) {
                            result += s;
                        }
                        while ((s = stdError.readLine()) != null) {
                            success = false;
                        }
                    } catch (IOException e) {
                        success = false;
                    }
                    break;
                }
                case DatabaseSystem.TYPE_MYSQL: {
                    try {
                        String s = null;
                        ProcessBuilder pb = new ProcessBuilder(new String[]{"mysqldump", "-u", username, "-p" + password, "-d", database.getName()});
                        Process p = pb.start();
                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        s = "";
                        while ((s = stdInput.readLine()) != null) {
                            result += s;
                        }
                        while ((s = stdError.readLine()) != null) {
                            success = false;
                        }
                    } catch (IOException e) {
                        success = false;
                    }
                    break;
                }
            }
        }
        return result;
    }

    public static File dumpCompleteDatabase(Database database, String username, String password) {
        File result = null;
        String fileName = System.currentTimeMillis() + "_" + database.getName() + ".sql";
        boolean success = false;
        if (getOperationSystem() == LINUX) {
            success = true;
            switch (database.getType()) {
                case DatabaseSystem.TYPE_POSTGRES: {
                    try {
                        String s = null;
                        ProcessBuilder pb = new ProcessBuilder(new String[]{"pg_dump", "-h", "localhost", "-U", username, "-F", "p", "--file=" + fileName, database.getName()});
                        Map<String, String> env = pb.environment();
                        env.put("PGPASSWORD", password);
                        Process p = pb.start();
                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        s = "";
                        while ((s = stdInput.readLine()) != null) {
                        }
                        while ((s = stdError.readLine()) != null) {
                            success = false;
                        }
                    } catch (IOException e) {
                        success = false;
                    }
                    break;
                }
                case DatabaseSystem.TYPE_MYSQL: {
                    try {
                        String s = null;
                        ProcessBuilder pb = new ProcessBuilder(new String[]{"mysqldump", "-u", username, "-p" + password, database.getName(), "--result-file=" + fileName});
                        Process p = pb.start();
                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        s = "";
                        while ((s = stdInput.readLine()) != null) {
                        }
                        while ((s = stdError.readLine()) != null) {
                            success = false;
                        }
                    } catch (IOException e) {
                        success = false;
                    }
                    break;
                }
            }
        }
        if (success) {
            result = new File(fileName);
        }
        return result;
    }

    public static boolean restoreCompleteDatabase(Database database, String username, String password, File dumpFile) {
        boolean success = false;
        if (getOperationSystem() == LINUX) {
            success = true;
            switch (database.getType()) {
                case DatabaseSystem.TYPE_POSTGRES: {
                    try {
                        String s = null;
                        ProcessBuilder pb = new ProcessBuilder(new String[]{"psql", "-h", "localhost", "-U", username, "-d", database.getName(), "-f", dumpFile.getAbsolutePath()});
                        Map<String, String> env = pb.environment();
                        env.put("PGPASSWORD", password);
                        Process p = pb.start();
                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        s = "";
                        while ((s = stdInput.readLine()) != null) {
                        }
                        while ((s = stdError.readLine()) != null) {
                            success = false;
                        }
                    } catch (IOException e) {
                        success = false;
                    }
                    break;
                }
                case DatabaseSystem.TYPE_MYSQL: {
                    try {
                        String s = null;
                        ProcessBuilder pb = new ProcessBuilder(new String[]{"sh", "-c", "mysql -u " + username + " -p" + password + " " + database.getName() + " < " + dumpFile.getAbsolutePath()});
                        Process p = pb.start();
                        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        s = "";
                        while ((s = stdInput.readLine()) != null) {
                        }
                        while ((s = stdError.readLine()) != null) {
                            success = false;
                        }
                    } catch (IOException e) {
                        System.out.println(e);
                        success = false;
                    }
                    break;
                }
            }
        }
        return success;
    }

    public static File downloadFile(String strUrl, String fileName) {
        File resultFile = null;
        try {
            URL url = new URL(strUrl);
            InputStream inputStream = url.openStream();
            FileOutputStream outputStream = new FileOutputStream(fileName);
            int i = 0;  
            byte[] buffer = new byte[1024];  
            while ((i = inputStream.read(buffer)) >= 0) {  
                outputStream.write(buffer, 0, i);  
            } 
            outputStream.close();
            inputStream.close();
            resultFile = new File(fileName);
        } catch (IOException ex) {
        }
        return resultFile;
    }

    public static void main(String[] args) {
    }
}