/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.jdbc;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.jdbc.relational.IoTDBRelationalDatabaseMetadata;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;

import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class IoTDBConnection implements Connection {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBConnection.class);
  private static final TSProtocolVersion protocolVersion =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;
  private static final String NOT_SUPPORT_PREPARE_CALL = "Does not support prepareCall";
  private static final String NOT_SUPPORT_PREPARE_STATEMENT = "Does not support prepareStatement";
  private static final String APACHE_IOTDB = "Apache IoTDB";
  private IClientRPCService.Iface client = null;
  private long sessionId = -1;
  private IoTDBConnectionParams params;
  private boolean isClosed = true;
  private SQLWarning warningChain = null;
  private TTransport transport;

  /**
   * Timeout of query can be set by users. Unit: s If not set, default value 0 will be used, which
   * will use server configuration.
   */
  private int queryTimeout = 0;

  /**
   * ConnectionTimeout and SocketTimeout. Unit: ms. If not set, default value 0 will be used, which
   * means that there's no timeout in the client side.
   */
  private int networkTimeout = Config.DEFAULT_CONNECTION_TIMEOUT_MS;

  private ZoneId zoneId;
  private Charset charset;

  private boolean autoCommit;
  private String url;

  public String getUserName() {
    return userName;
  }

  private String userName;

  // default is tree
  public String getSqlDialect() {
    if (params != null && StringUtils.isNotBlank(params.getSqlDialect())) {
      return params.getSqlDialect();
    } else {
      return "tree";
    }
  }

  // ms is 1_000, us is 1_000_000, ns is 1_000_000_000
  private int timeFactor = 1_000;

  public IoTDBConnection() {
    // allowed to create an instance without parameter input.
  }

  public IoTDBConnection(String url, Properties info) throws SQLException, TTransportException {
    if (url == null) {
      throw new IoTDBURLException("Input url cannot be null");
    }
    params = Utils.parseUrl(url, info);
    this.url = url;
    this.userName = info.get("user").toString();
    this.networkTimeout = params.getNetworkTimeout();
    this.zoneId = ZoneId.of(params.getTimeZone());
    this.charset = params.getCharset();
    openTransport();
    if (Config.rpcThriftCompressionEnable) {
      setClient(new IClientRPCService.Client(new TCompactProtocol(transport)));
    } else {
      setClient(new IClientRPCService.Client(new TBinaryProtocol(transport)));
    }
    // open client session
    openSession();
    // Wrap the client with a thread-safe proxy to serialize the RPC calls
    setClient(RpcUtils.newSynchronizedClient(getClient()));
    autoCommit = false;
  }

  public String getUrl() {
    return url;
  }

  public IoTDBConnectionParams getParams() {
    return params;
  }

  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    throw new SQLException("Does not support isWrapperFor");
  }

  @Override
  public <T> T unwrap(Class<T> arg0) throws SQLException {
    throw new SQLException("Does not support unwrap");
  }

  @Override
  public void abort(Executor arg0) throws SQLException {
    throw new SQLException("Does not support abort");
  }

  @Override
  public void clearWarnings() {
    warningChain = null;
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }
    TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
    try {
      getClient().closeSession(req);
    } catch (TException e) {
      throw new SQLException(
          "Error occurs when closing session at server. Maybe server is down.", e);
    } finally {
      isClosed = true;
      if (transport != null) {
        transport.close();
      }
    }
  }

  @Override
  public void commit() throws SQLException {}

  @Override
  public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
    throw new SQLException("Does not support createArrayOf");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLException("Does not support createBlob");
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLException("Does not support createClob");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLException("Does not suppport createNClob");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLException("Does not support createSQLXML");
  }

  @Override
  public Statement createStatement() throws SQLException {
    if (isClosed) {
      throw new SQLException("Cannot create statement because connection is closed");
    }
    return new IoTDBStatement(this, getClient(), sessionId, zoneId, charset, queryTimeout);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLException(
          String.format(
              "Statements with result set concurrency %d are not supported", resultSetConcurrency));
    }
    if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
      throw new SQLException(
          String.format("Statements with ResultSet type %d are not supported", resultSetType));
    }
    return new IoTDBStatement(this, getClient(), sessionId, zoneId, charset, queryTimeout);
  }

  @Override
  public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {
    throw new SQLException("Does not support createStatement");
  }

  @Override
  public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
    throw new SQLException("Does not support createStruct");
  }

  @Override
  public boolean getAutoCommit() {
    return autoCommit;
  }

  @Override
  public void setAutoCommit(boolean arg0) {
    autoCommit = arg0;
  }

  @Override
  public String getCatalog() {
    return APACHE_IOTDB;
  }

  @Override
  public void setCatalog(String arg0) throws SQLException {
    if (getSqlDialect().equals(Constant.TABLE_DIALECT)) {
      if (APACHE_IOTDB.equals(arg0)) {
        return;
      }
      for (String str : IoTDBRelationalDatabaseMetadata.allIotdbTableSQLKeywords) {
        if (arg0.equalsIgnoreCase(str)) {
          arg0 = "\"" + arg0 + "\"";
        }
      }

      Statement stmt = this.createStatement();
      String sql = "USE " + arg0;
      boolean rs;
      try {
        rs = stmt.execute(sql);
      } catch (SQLException e) {
        stmt.close();
        logger.error("Use database error: {}", e.getMessage());
        throw e;
      }
    }
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new SQLException("Does not support getClientInfo");
  }

  @Override
  public void setClientInfo(Properties arg0) throws SQLClientInfoException {
    throw new SQLClientInfoException("Does not support setClientInfo", null);
  }

  @Override
  public String getClientInfo(String arg0) throws SQLException {
    throw new SQLException("Does not support getClientInfo");
  }

  @Override
  public int getHoldability() {
    return 0;
  }

  @Override
  public void setHoldability(int arg0) throws SQLException {
    throw new SQLException("Does not support setHoldability");
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    if (isClosed) {
      throw new SQLException("Cannot create statement because connection is closed");
    }
    if (getSqlDialect().equals(Constant.TABLE_DIALECT)) {
      return new IoTDBRelationalDatabaseMetadata(this, getClient(), sessionId, zoneId);
    }
    return new IoTDBDatabaseMetadata(this, getClient(), sessionId, zoneId);
  }

  @Override
  public int getNetworkTimeout() {
    return networkTimeout;
  }

  @Override
  public String getSchema() throws SQLException {
    if (getSqlDialect().equals(Constant.TABLE_DIALECT)) {
      return getDatabase();
    }
    throw new SQLException("Does not support getSchema");
  }

  @Override
  public void setSchema(String arg0) throws SQLException {
    // changeDefaultDatabase(arg0);
    if (getSqlDialect().equals(Constant.TABLE_DIALECT)) {
      for (String str : IoTDBRelationalDatabaseMetadata.allIotdbTableSQLKeywords) {
        if (arg0.equalsIgnoreCase(str)) {
          arg0 = "\"" + arg0 + "\"";
        }
      }

      Statement stmt = this.createStatement();
      String sql = "USE " + arg0;
      boolean rs;
      try {
        rs = stmt.execute(sql);
      } catch (SQLException e) {
        stmt.close();
        logger.error("Use database error: {}", e.getMessage());
        throw e;
      }
    }
  }

  @Override
  public int getTransactionIsolation() {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public void setTransactionIsolation(int arg0) throws SQLException {
    throw new SQLException("Does not support setTransactionIsolation");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLException("Does not support getTypeMap");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
    throw new SQLException("Does not support setTypeMap");
  }

  @Override
  public SQLWarning getWarnings() {
    return warningChain;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public void setReadOnly(boolean readonly) throws SQLException {
    if (readonly) {
      throw new SQLException("Does not support readOnly");
    }
  }

  @Override
  public boolean isValid(int arg0) {
    return !isClosed;
  }

  @Override
  public String nativeSQL(String arg0) throws SQLException {
    throw new SQLException("Does not support nativeSQL");
  }

  @Override
  public CallableStatement prepareCall(String arg0) throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_CALL);
  }

  @Override
  public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_CALL);
  }

  @Override
  public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3)
      throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_CALL);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new IoTDBPreparedStatement(this, getClient(), sessionId, sql, zoneId, charset);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public void releaseSavepoint(Savepoint arg0) throws SQLException {
    throw new SQLException("Does not support releaseSavepoint");
  }

  @Override
  public void rollback() {
    // do nothing in rollback
  }

  @Override
  public void rollback(Savepoint arg0) {
    // do nothing in rollback
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    if (name.equalsIgnoreCase("time_zone")) {
      try {
        setTimeZone(value);
      } catch (TException | IoTDBSQLException e) {
        throw new SQLClientInfoException("Set time_zone error: ", null, e);
      }
    } else {
      HashMap<String, ClientInfoStatus> hashMap = new HashMap<>();
      hashMap.put(name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
      throw new SQLClientInfoException("Does not support this type of client info: ", hashMap);
    }
  }

  @Override
  public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
    throw new SQLException("Does not support setNetworkTimeout");
  }

  public int getQueryTimeout() {
    return this.queryTimeout;
  }

  public void setQueryTimeout(int seconds) throws SQLException {
    if (seconds < 0) {
      throw new SQLException(String.format("queryTimeout %d must be >= 0!", seconds));
    }
    this.queryTimeout = seconds;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLException("Does not support setSavepoint");
  }

  @Override
  public Savepoint setSavepoint(String arg0) throws SQLException {
    throw new SQLException("Does not support setSavepoint");
  }

  public IClientRPCService.Iface getClient() {
    return client;
  }

  public long getSessionId() {
    return sessionId;
  }

  public void setClient(IClientRPCService.Iface client) {
    this.client = client;
  }

  private void openTransport() throws TTransportException {
    DeepCopyRpcTransportFactory.setDefaultBufferCapacity(params.getThriftDefaultBufferSize());
    DeepCopyRpcTransportFactory.setThriftMaxFrameSize(params.getThriftMaxFrameSize());

    if (params.isUseSSL()) {
      transport =
          DeepCopyRpcTransportFactory.INSTANCE.getTransport(
              params.getHost(),
              params.getPort(),
              getNetworkTimeout(),
              params.getTrustStore(),
              params.getTrustStorePwd());
    } else {
      transport =
          DeepCopyRpcTransportFactory.INSTANCE.getTransport(
              params.getHost(), params.getPort(), getNetworkTimeout());
    }
    if (!transport.isOpen()) {
      transport.open();
    }
  }

  private void openSession() throws SQLException {
    TSOpenSessionReq openReq = new TSOpenSessionReq();

    openReq.setUsername(params.getUsername());
    openReq.setPassword(params.getPassword());
    openReq.setZoneId(getTimeZone());
    openReq.putToConfiguration(Config.VERSION, params.getVersion().toString());
    openReq.putToConfiguration(Config.SQL_DIALECT, params.getSqlDialect());
    params.getDb().ifPresent(db -> openReq.putToConfiguration(Config.DATABASE, db));

    TSOpenSessionResp openResp = null;
    try {
      openResp = client.openSession(openReq);
      sessionId = openResp.getSessionId();
      // validate connection
      RpcUtils.verifySuccess(openResp.getStatus());

      this.timeFactor = RpcUtils.getTimeFactor(openResp);
      if (protocolVersion.getValue() != openResp.getServerProtocolVersion().getValue()) {
        logger.warn(
            "Protocol differ, Client version is {}}, but Server version is {}",
            protocolVersion.getValue(),
            openResp.getServerProtocolVersion().getValue());
        if (openResp.getServerProtocolVersion().getValue() == 0) { // less than 0.10
          throw new TException(
              String.format(
                  "Protocol not supported, Client version is %s, but Server version is %s",
                  protocolVersion.getValue(), openResp.getServerProtocolVersion().getValue()));
        }
      }
      logger.info(openResp.getStatus().getMessage());
    } catch (TException e) {
      transport.close();
      if (e.getMessage().contains("Required field 'client_protocol' was not present!")) {
        // the server is an old version (less than 0.10)
        throw new SQLException(
            String.format(
                "Can not establish connection with %s : You may try to connect an old version IoTDB instance using a client with new version: %s. ",
                params.getJdbcUriString(), e.getMessage()),
            e);
      }
      throw new SQLException(
          String.format(
              "Can not establish connection with %s : %s. ",
              params.getJdbcUriString(), e.getMessage()),
          e);
    } catch (StatementExecutionException e) {
      // failed to connect, disconnect from the server
      transport.close();
      throw new IoTDBSQLException(e.getMessage(), openResp.getStatus());
    }
    isClosed = false;
  }

  public boolean reconnect() {
    boolean flag = false;
    for (int i = 1; i <= Config.RETRY_NUM; i++) {
      try {
        if (transport != null) {
          transport.close();
          openTransport();
          if (Config.rpcThriftCompressionEnable) {
            setClient(new IClientRPCService.Client(new TCompactProtocol(transport)));
          } else {
            setClient(new IClientRPCService.Client(new TBinaryProtocol(transport)));
          }
          openSession();
          setClient(RpcUtils.newSynchronizedClient(getClient()));
          flag = true;
          break;
        }
      } catch (Exception e) {
        try {
          Thread.sleep(Config.RETRY_INTERVAL_MS);
        } catch (InterruptedException e1) {
          logger.error("reconnect is interrupted.", e1);
          Thread.currentThread().interrupt();
        }
      }
    }
    return flag;
  }

  public String getTimeZone() {
    if (zoneId == null) {
      zoneId = ZoneId.systemDefault();
    }
    return zoneId.toString();
  }

  public void setTimeZone(String timeZone) throws TException, IoTDBSQLException {
    TSSetTimeZoneReq req = new TSSetTimeZoneReq(sessionId, timeZone);
    TSStatus resp = getClient().setTimeZone(req);
    try {
      RpcUtils.verifySuccess(resp);
    } catch (StatementExecutionException e) {
      throw new IoTDBSQLException(e.getMessage(), resp);
    }
    this.zoneId = ZoneId.of(timeZone);
  }

  public ServerProperties getServerProperties() throws TException {
    return getClient().getProperties();
  }

  protected void changeDefaultDatabase(String database) {
    params.setDb(database);
  }

  protected void mayChangeDefaultSqlDialect(String sqlDialect) {
    if (!sqlDialect.equals(params.getSqlDialect())) {
      params.setSqlDialect(sqlDialect);
      params.setDb(null);
    }
  }

  public int getTimeFactor() {
    return timeFactor;
  }

  public String getDatabase() {
    return params.getDb().orElse(null);
  }
}
