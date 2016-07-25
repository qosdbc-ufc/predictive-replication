package qosdbc.jdbc.driver;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import qosdbc.commons.OutputMessage;
import qosdbc.commons.jdbc.RequestCode;
import qosdbc.commons.jdbc.QoSDBCMessage.*;

/**
 *
 * @author Leonardo Oliveira Moreira
 *
 */
public class QoSDBCConnection implements Connection {

  private String host;
  private int port;
  private Socket socket;
  private ObjectOutputStream outputStream;
  private ObjectInputStream inputStream;
  private boolean autoCommit, closed, readOnly;
  private Properties props;
  private Long slaResponseTime;
  private QoSDBCDatabaseMetaData databaseMetaData;
  private long id;
  private long currentTransactionId;

  public QoSDBCConnection(String host, int port, Properties props) throws SQLException {
    this.id = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    this.host = host;
    this.port = port;
    this.props = props;
    if (props != null) {
      String slaResponseTimeStr = props.getProperty("sla.response_time");
      if (slaResponseTimeStr != null) {
        try {
          long value = Long.parseLong(slaResponseTimeStr);
          this.slaResponseTime = value;
        } catch (Exception ex) {
          this.slaResponseTime = null;
        }
      }
    }
    try {
      socket = new Socket(host, port);
      socket.setTcpNoDelay(true);
      outputStream = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()));
      outputStream.flush();
      inputStream = new ObjectInputStream(new BufferedInputStream((socket.getInputStream())));
    } catch (IOException e) {
      throw new SQLException("unable to connect to database: " + e.getMessage());
    }
    synchronized (this) {
      Request.Builder request = Request.newBuilder();
      request.setDatabase(props.getProperty("databaseName"));
      request.setCode(RequestCode.SQL_CONNECTION_CREATE);
      Response response = executeMessage(request);
      if (response == null || response.getState() == RequestCode.STATE_FAILURE) {
        throw new SQLException("Failed to open connection");
      }
      currentTransactionId = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }
    this.autoCommit = true;
    this.readOnly = false;
    this.databaseMetaData = new QoSDBCDatabaseMetaData();
  }

  public long getId() {
    return id;
  }

  /**
   *
   * @param request
   * @return
   * @throws SQLException
   */
  public synchronized Response executeMessage(Request.Builder request) throws SQLException {
    if (slaResponseTime != null) {
      request.setSlaResponseTime(slaResponseTime);
    }
    request.setConnectionId(id);
    request.setTransactionId(currentTransactionId);
    Response response = null;
    try {
      request.build().writeDelimitedTo(outputStream);
      outputStream.flush();
      response = Response.parseDelimitedFrom(inputStream);
    } catch (IOException e) {
      throw new SQLException(e.toString());
    }
    if (response == null || response.getState() != RequestCode.STATE_SUCCESS) {
      OutputMessage.println("ERROR CRITICO: COMMANDO: " + request.getCommand() + "response.getState(): " + response.getState() + " CODE: " + request.getCode() + " response: " + request);
      OutputMessage.println("ERROR: " + (request != null && request.getCommand().trim().length() > 0 ? request.getCommand() : "Invalid query"));
    }
    return response;
  }

  protected void finalize() throws Throwable {
    if (!closed) close();
  }

  @Override
  public synchronized Statement createStatement() throws SQLException {
    QoSDBCStatement statement = new QoSDBCStatement(this);
    Request.Builder request = Request.newBuilder();
    request.setCode(RequestCode.SQL_STATEMENT_CREATE);
    HashMap<String, String> parametersMap = new HashMap<String, String>();
    parametersMap.put("statementId", String.valueOf(statement.getStatementID()));
    request.putAllParameters(parametersMap);
    request.setDatabase(getDatabaseName());
    Response response = executeMessage(request);
    if (response == null || response.getState() == RequestCode.STATE_FAILURE) {
      throw new SQLException("Failed to close statement");
    }
    return statement;
  }

  @Override
  public synchronized PreparedStatement prepareStatement(String sql) throws SQLException {
    QoSDBCPreparedStatement preparedStatement = new QoSDBCPreparedStatement(this, sql);
    Request.Builder request = Request.newBuilder();
    request.setCode(RequestCode.SQL_STATEMENT_CREATE);
    HashMap<String, String> parametersMap = new HashMap<String, String>();
    parametersMap.put("statementId", String.valueOf(preparedStatement.getStatementID()));
    request.putAllParameters(parametersMap);
    request.setDatabase(getDatabaseName());
    Response response = executeMessage(request);
    if (response == null || response.getState() == RequestCode.STATE_FAILURE) {
      throw new SQLException("Failed to close statement");
    }
    return preparedStatement;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return sql;
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return this.autoCommit;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (autoCommit == this.autoCommit) return;
    this.autoCommit = autoCommit;

    Request.Builder request = Request.newBuilder();
    request.setCode(RequestCode.CONNECTION_CHANGE_AUTOCOMMIT);
    request.setDatabase(props.getProperty("databaseName"));
    HashMap<String, String> parametersMap = new HashMap<String, String>();
    parametersMap.put("autoCommit", String.valueOf(autoCommit));
    request.putAllParameters(parametersMap);
    Response response = executeMessage(request);

    if (response == null || response.getState() == RequestCode.STATE_FAILURE) {
      this.autoCommit = !autoCommit;
      throw new SQLException("Failed to commit connection");
    }
  }

  @Override
  public synchronized void commit() throws SQLException {

    Request.Builder request = Request.newBuilder();
    request.setDatabase(props.getProperty("databaseName"));
    request.setCode(RequestCode.SQL_COMMIT);
    Response response = executeMessage(request);
    if (response == null || response.getState() == RequestCode.STATE_FAILURE) {
      throw new SQLException("Failed to commit connection");
    }
    currentTransactionId = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
  }

  @Override
  public synchronized void rollback() throws SQLException {

    Request.Builder request = Request.newBuilder();
    request.setDatabase(props.getProperty("databaseName"));
    request.setCode(RequestCode.SQL_ROLLBACK);
    Response response = executeMessage(request);
    if (response == null || response.getState() == RequestCode.STATE_FAILURE) {
      throw new SQLException("Failed to commit connection");
    }
    currentTransactionId = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
  }

  @Override
  public synchronized void close() throws SQLException {
    try {
      Request.Builder request = Request.newBuilder();
      request.setDatabase(props.getProperty("databaseName"));
      request.setCode(RequestCode.SQL_CONNECTION_CLOSE);
      Response response = executeMessage(request);
      if (response == null || response.getState() == RequestCode.STATE_FAILURE) {
        throw new SQLException("Failed to close connection");
      }
    } finally {
      closed = true;
      try {
        socket.close();
      } catch (IOException e) {
        throw new SQLException("close failed: " + e.getMessage());
      }
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return databaseMetaData;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return this.readOnly;
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    if (readOnly == this.readOnly) return;
    this.readOnly = readOnly;
  }

  @Override
  public String getCatalog() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_SERIALIZABLE;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    QoSDBCPreparedStatement preparedStatement = new QoSDBCPreparedStatement(this, sql);
    Request.Builder request = Request.newBuilder();
    request.setCode(RequestCode.SQL_STATEMENT_CREATE);
    HashMap<String, String> parametersMap = new HashMap<String, String>();
    parametersMap.put("statementId", String.valueOf(preparedStatement.getStatementID()));
    request.putAllParameters(parametersMap);
    request.setDatabase(getDatabaseName());
    Response response = executeMessage(request);
    if (response == null || response.getState() == RequestCode.STATE_FAILURE) {
      throw new SQLException("Failed to close statement");
    }
    return preparedStatement;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return !this.closed;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public String getDatabaseName() {
    return props.getProperty("databaseName");
  }

  @Override
  public String getSchema() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setSchema(String string) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void abort(Executor exctr) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setNetworkTimeout(Executor exctr, int i) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}