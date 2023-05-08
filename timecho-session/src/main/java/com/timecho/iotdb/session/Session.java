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
package com.timecho.iotdb.session;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.LicenseInfoResp;
import org.apache.iotdb.service.rpc.thrift.WhiteListInfoResp;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import com.timecho.iotdb.isession.ISession;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.thrift.TException;

import java.time.ZoneId;
import java.util.List;
import java.util.Set;

import static com.timecho.iotdb.session.SQLConstants.ALL_DB_SQL;
import static com.timecho.iotdb.session.SQLConstants.COMMA;
import static com.timecho.iotdb.session.SQLConstants.SPECIFY_DB_SQL_DATABASE;
import static com.timecho.iotdb.session.SQLConstants.SPECIFY_DB_SQL_FLUSH;
import static com.timecho.iotdb.session.SQLConstants.SPECIFY_DB_SQL_PREFIX;
import static com.timecho.iotdb.session.SQLConstants.SPECIFY_DB_SQL_SUFFIX;
import static com.timecho.iotdb.session.SQLConstants.SQL_TIME_GT;
import static com.timecho.iotdb.session.SQLConstants.SQL_TIME_LT;

public class Session extends org.apache.iotdb.session.Session implements ISession {
  public Session(String host, int rpcPort) {
    super(host, rpcPort);
  }

  public Session(String host, String rpcPort, String username, String password) {
    super(host, rpcPort, username, password);
  }

  public Session(String host, int rpcPort, String username, String password) {
    super(host, rpcPort, username, password);
  }

  public Session(String host, int rpcPort, String username, String password, int fetchSize) {
    super(host, rpcPort, username, password, fetchSize);
  }

  public Session(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      long queryTimeoutInMs) {
    super(host, rpcPort, username, password, fetchSize, queryTimeoutInMs);
  }

  public Session(String host, int rpcPort, String username, String password, ZoneId zoneId) {
    super(host, rpcPort, username, password, zoneId);
  }

  public Session(
      String host, int rpcPort, String username, String password, boolean enableRedirection) {
    super(host, rpcPort, username, password, enableRedirection);
  }

  public Session(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      boolean enableRedirection) {
    super(host, rpcPort, username, password, fetchSize, zoneId, enableRedirection);
  }

  public Session(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean enableRedirection,
      Version version) {
    super(
        host,
        rpcPort,
        username,
        password,
        fetchSize,
        zoneId,
        thriftDefaultBufferSize,
        thriftMaxFrameSize,
        enableRedirection,
        version);
  }

  public Session(List<String> nodeUrls, String username, String password) {
    super(nodeUrls, username, password);
  }

  public Session(List<String> nodeUrls, String username, String password, int fetchSize) {
    super(nodeUrls, username, password, fetchSize);
  }

  public Session(List<String> nodeUrls, String username, String password, ZoneId zoneId) {
    super(nodeUrls, username, password, zoneId);
  }

  public Session(
      List<String> nodeUrls,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean enableRedirection,
      Version version) {
    super(
        nodeUrls,
        username,
        password,
        fetchSize,
        zoneId,
        thriftDefaultBufferSize,
        thriftMaxFrameSize,
        enableRedirection,
        version);
  }

  @Override
  public WhiteListInfoResp getWhiteIpSet()
      throws IoTDBConnectionException, StatementExecutionException {
    WhiteListInfoResp resp;
    try {
      resp = defaultSessionConnection.getClient().getWhiteIpSet();
      RpcUtils.verifySuccess(resp.getStatus());
    } catch (TException e) {
      if (defaultSessionConnection.reconnect()) {
        try {
          resp = defaultSessionConnection.getClient().getWhiteIpSet();
          RpcUtils.verifySuccess(resp.getStatus());
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(defaultSessionConnection.logForReconnectionFailure());
      }
    }
    return resp;
  }

  @Override
  public void updateWhiteList(Set<String> ipSet)
      throws IoTDBConnectionException, StatementExecutionException {
    try {

      RpcUtils.verifySuccess(defaultSessionConnection.getClient().updateWhiteList(ipSet));
    } catch (TException e) {
      if (defaultSessionConnection.reconnect()) {
        try {
          RpcUtils.verifySuccess(defaultSessionConnection.getClient().updateWhiteList(ipSet));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(defaultSessionConnection.logForReconnectionFailure());
      }
    }
  }

  @Override
  public LicenseInfoResp getLicenseInfo()
      throws StatementExecutionException, IoTDBConnectionException {
    LicenseInfoResp resp;
    try {
      resp = defaultSessionConnection.getClient().getLicenseInfo();

      RpcUtils.verifySuccess(resp.getStatus());
    } catch (TException e) {
      if (defaultSessionConnection.reconnect()) {
        try {
          resp = defaultSessionConnection.getClient().getLicenseInfo();
          RpcUtils.verifySuccess(resp.getStatus());
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(defaultSessionConnection.logForReconnectionFailure());
      }
    }
    return resp;
  }

  @Override
  public long getTotalPoints(Set<String> databaseSet)
      throws StatementExecutionException, IoTDBConnectionException {
    StringBuilder finalSql = buildQuerySql(databaseSet).append(SPECIFY_DB_SQL_SUFFIX);
    return getTotalPointsFromDataset(finalSql);
  }

  @Override
  public long getTotalPoints(Set<String> databaseSet, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException {
    StringBuilder finalSql =
        buildQuerySql(databaseSet)
            .append(SQL_TIME_GT)
            .append(startTime)
            .append(SQL_TIME_LT)
            .append(endTime)
            .append(SPECIFY_DB_SQL_SUFFIX);
    return getTotalPointsFromDataset(finalSql);
  }

  @Override
  public long getTotalPoints(Set<String> databaseSet, long startTime)
      throws StatementExecutionException, IoTDBConnectionException {
    StringBuilder finalSql =
        buildQuerySql(databaseSet)
            .append(SQL_TIME_GT)
            .append(startTime)
            .append(SPECIFY_DB_SQL_SUFFIX);
    return getTotalPointsFromDataset(finalSql);
  }

  private long getTotalPointsFromDataset(StringBuilder baseSql)
      throws IoTDBConnectionException, StatementExecutionException {
    try (SessionDataSet sessionDataSet = executeQueryStatement(baseSql.toString())) {
      if (sessionDataSet.hasNext()) {
        RowRecord next = sessionDataSet.next();
        if (next.hasNullField()) {
          return 0;
        }
        return (long) next.getFields().get(0).getDoubleV();
      }
    }
    return 0;
  }

  private static StringBuilder buildQuerySql(Set<String> databaseSet) {
    StringBuilder baseSql = new StringBuilder(ALL_DB_SQL);
    if (!CollectionUtils.isEmpty(databaseSet)) {
      baseSql = new StringBuilder(SPECIFY_DB_SQL_PREFIX);
      for (String database : databaseSet) {
        baseSql
            .append(SPECIFY_DB_SQL_DATABASE)
            .append(database)
            .append(SPECIFY_DB_SQL_FLUSH)
            .append(COMMA);
      }
      baseSql.deleteCharAt(baseSql.length() - 1);
    }
    return baseSql;
  }

  public static class Builder {
    private String host = SessionConfig.DEFAULT_HOST;
    private int rpcPort = SessionConfig.DEFAULT_PORT;
    private String username = SessionConfig.DEFAULT_USER;
    private String password = SessionConfig.DEFAULT_PASSWORD;
    private int fetchSize = SessionConfig.DEFAULT_FETCH_SIZE;
    private ZoneId zoneId = null;
    private int thriftDefaultBufferSize = SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY;
    private int thriftMaxFrameSize = SessionConfig.DEFAULT_MAX_FRAME_SIZE;
    private boolean enableRedirection = SessionConfig.DEFAULT_REDIRECTION_MODE;
    private Version version = SessionConfig.DEFAULT_VERSION;
    private long timeOut = SessionConfig.DEFAULT_QUERY_TIME_OUT;

    private List<String> nodeUrls = null;

    public Session.Builder host(String host) {
      this.host = host;
      return this;
    }

    public Session.Builder port(int port) {
      this.rpcPort = port;
      return this;
    }

    public Session.Builder username(String username) {
      this.username = username;
      return this;
    }

    public Session.Builder password(String password) {
      this.password = password;
      return this;
    }

    public Session.Builder fetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    public Session.Builder zoneId(ZoneId zoneId) {
      this.zoneId = zoneId;
      return this;
    }

    public Session.Builder thriftDefaultBufferSize(int thriftDefaultBufferSize) {
      this.thriftDefaultBufferSize = thriftDefaultBufferSize;
      return this;
    }

    public Session.Builder thriftMaxFrameSize(int thriftMaxFrameSize) {
      this.thriftMaxFrameSize = thriftMaxFrameSize;
      return this;
    }

    public Session.Builder enableRedirection(boolean enableRedirection) {
      this.enableRedirection = enableRedirection;
      return this;
    }

    public Session.Builder nodeUrls(List<String> nodeUrls) {
      this.nodeUrls = nodeUrls;
      return this;
    }

    public Session.Builder version(Version version) {
      this.version = version;
      return this;
    }

    public Session.Builder timeOut(long timeOut) {
      this.timeOut = timeOut;
      return this;
    }

    public Session build() {
      if (nodeUrls != null
          && (!SessionConfig.DEFAULT_HOST.equals(host) || rpcPort != SessionConfig.DEFAULT_PORT)) {
        throw new IllegalArgumentException(
            "You should specify either nodeUrls or (host + rpcPort), but not both");
      }

      if (nodeUrls != null) {
        Session newSession =
            new Session(
                nodeUrls,
                username,
                password,
                fetchSize,
                zoneId,
                thriftDefaultBufferSize,
                thriftMaxFrameSize,
                enableRedirection,
                version);
        newSession.setEnableQueryRedirection(true);
        return newSession;
      }

      return new Session(
          host,
          rpcPort,
          username,
          password,
          fetchSize,
          zoneId,
          thriftDefaultBufferSize,
          thriftMaxFrameSize,
          enableRedirection,
          version);
    }
  }
}
