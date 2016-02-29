/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qosdbc.coordinator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import qosdbc.commons.OutputMessage;

/**
 *
 * @author Leonardo Oliveira Moreira
 */
public class QoSDBCBackupOrganizer extends Thread {

    private boolean executing = false;

    public QoSDBCBackupOrganizer() {
        //setPriority(MIN_PRIORITY);
    }

    @Override
    public void run() {
        executing = true;
        // creates or reference the dump directory
        File dumpDir = new File(System.getProperty("user.dir") + System.getProperty("file.separator") + "dump");
        if (!dumpDir.exists()) {
            dumpDir.mkdir();
        }

        while (executing) {
            // selects all the sql files
            //--
            File currentDir = new File(System.getProperty("user.dir"));
            List<File> sqlFileList = new ArrayList<File>();
            for (File f : currentDir.listFiles()) {
                if (f.getName().endsWith(".sql")) {
                    sqlFileList.add(f);
                }
            }
            if (sqlFileList != null) {
                Map<String, List<File>> databaseList = new HashMap<String, List<File>>();
                for (File f : sqlFileList) {
                    String database = f.getName().substring(f.getName().lastIndexOf("_") + 1, f.getName().length() - ".sql".length());
                    if (!databaseList.containsKey(database)) {
                        List<File> fileList = new ArrayList<File>();
                        fileList.add(f);
                        databaseList.put(database, fileList);
                    } else {
                        List<File> fileList = databaseList.get(database);
                        fileList.add(f);
                        databaseList.put(database, fileList);
                    }
                }
                //--
                // get the database data for all databases, and put this data to memory and delete log files
                // --
                Set<String> databaseNameListSet = databaseList.keySet();
                Iterator<String> databaseNameListIterator = databaseNameListSet.iterator();
                while (databaseNameListIterator.hasNext()) {
                    String databaseKey = databaseNameListIterator.next();
                    List<File> databaseFileLog = databaseList.remove(databaseKey);
                    List<SQLInstruction> sqlInstructions = new ArrayList<SQLInstruction>();
                    for (File f : databaseFileLog) {
                        try {
                            BufferedReader reader = new BufferedReader(new FileReader(f));
                            String s = null;
                            while ((s = reader.readLine()) != null) {
                                String[] sArray = s.split(";");
                                SQLInstruction sqli = new SQLInstruction(Long.parseLong(sArray[0]), Long.parseLong(sArray[1]), sArray[2]);
                                sqlInstructions.add(sqli);
                            }
                            reader.close();
                        } catch (IOException ex) {
                            OutputMessage.println(ex.getMessage());
                            continue;
                        }
                        f.delete();
                    }
                    //--
                    // creates the dump file
                    //--
                    File databaseDumpFile = new File(dumpDir + System.getProperty("file.separator") + System.currentTimeMillis() + "_" + databaseKey + ".sql");
                    try {
                        BufferedWriter databaseDumpWriter = new BufferedWriter(new FileWriter(databaseDumpFile, true));
                        Set<SQLInstruction> treeSet = new TreeSet<SQLInstruction>(new SQLInstructionComparator());
                        treeSet.addAll(sqlInstructions);
                        Iterator<SQLInstruction> treeIterator = treeSet.iterator();
                        while (treeIterator.hasNext()) {
                            SQLInstruction sqli = treeIterator.next();
                            databaseDumpWriter.append(sqli.toString() + "\n");
                            databaseDumpWriter.flush();
                        }
                        databaseDumpWriter.close();
                    } catch (IOException ex) {
                        OutputMessage.println(ex.getMessage());
                    }
                    //--
                } // WHILE DATABASE LIST (databaseNameListIterator)
            } // IF EXISTS FILE TO PROCESS
            try {
                sleep(60000);
            } catch (InterruptedException ex) {
            }
        }
    }

    public boolean isExecuting() {
        return executing;
    }

    public void setExecuting(boolean executing) {
        this.executing = executing;
    }

    public static void main(String[] args) {
    }
}

class SQLInstruction {

    private long timestamp;
    private long connectionId;
    private String sql;

    public SQLInstruction(long timestamp, long connectionId, String sql) {
        this.timestamp = timestamp;
        this.connectionId = connectionId;
        this.sql = sql;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(long connectionId) {
        this.connectionId = connectionId;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    @Override
    public String toString() {
        return timestamp + ";" + connectionId + ";" + sql;
    }
}

class SQLInstructionComparator implements Comparator<SQLInstruction> {

    @Override
    public int compare(SQLInstruction o1, SQLInstruction o2) {
        return (o1.getTimestamp() < o2.getTimestamp() ? -1 : (o1.getTimestamp() == o2.getTimestamp() ? 0 : 1));
    }
}