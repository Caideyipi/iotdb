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
package com.timecho.timechodb.session.pool;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.LicenseInfoResp;
import org.apache.iotdb.service.rpc.thrift.WhiteListInfoResp;

import com.timecho.timechodb.isession.ISession;
import com.timecho.timechodb.isession.pool.ISessionPool;
import com.timecho.timechodb.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.List;
import java.util.Set;

public class SessionPool extends org.apache.iotdb.session.pool.SessionPool implements ISessionPool {
  private static final Logger logger = LoggerFactory.getLogger(SessionPool.class);

  public SessionPool(String host, int port, String user, String password, int maxSize) {
    super(host, port, user, password, maxSize);
  }

  public SessionPool(List<String> nodeUrls, String user, String password, int maxSize) {
    super(nodeUrls, user, password, maxSize);
  }

  public SessionPool(
      String host, int port, String user, String password, int maxSize, boolean enableCompression) {
    super(host, port, user, password, maxSize, enableCompression);
  }

  public SessionPool(
      List<String> nodeUrls, String user, String password, int maxSize, boolean enableCompression) {
    super(nodeUrls, user, password, maxSize, enableCompression);
  }

  public SessionPool(
      String host,
      int port,
      String user,
      String password,
      int maxSize,
      boolean enableCompression,
      boolean enableRedirection) {
    super(host, port, user, password, maxSize, enableCompression, enableRedirection);
  }

  public SessionPool(
      List<String> nodeUrls,
      String user,
      String password,
      int maxSize,
      boolean enableCompression,
      boolean enableRedirection) {
    super(nodeUrls, user, password, maxSize, enableCompression, enableRedirection);
  }

  public SessionPool(
      String host, int port, String user, String password, int maxSize, ZoneId zoneId) {
    super(host, port, user, password, maxSize, zoneId);
  }

  public SessionPool(
      List<String> nodeUrls, String user, String password, int maxSize, ZoneId zoneId) {
    super(nodeUrls, user, password, maxSize, zoneId);
  }

  public SessionPool(
      String host,
      int port,
      String user,
      String password,
      int maxSize,
      int fetchSize,
      long waitToGetSessionTimeoutInMs,
      boolean enableCompression,
      ZoneId zoneId,
      boolean enableRedirection,
      int connectionTimeoutInMs,
      Version version,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize) {
    super(
        host,
        port,
        user,
        password,
        maxSize,
        fetchSize,
        waitToGetSessionTimeoutInMs,
        enableCompression,
        zoneId,
        enableRedirection,
        connectionTimeoutInMs,
        version,
        thriftDefaultBufferSize,
        thriftMaxFrameSize);
  }

  public SessionPool(
      List<String> nodeUrls,
      String user,
      String password,
      int maxSize,
      int fetchSize,
      long waitToGetSessionTimeoutInMs,
      boolean enableCompression,
      ZoneId zoneId,
      boolean enableRedirection,
      int connectionTimeoutInMs,
      Version version,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize) {
    super(
        nodeUrls,
        user,
        password,
        maxSize,
        fetchSize,
        waitToGetSessionTimeoutInMs,
        enableCompression,
        zoneId,
        enableRedirection,
        connectionTimeoutInMs,
        version,
        thriftDefaultBufferSize,
        thriftMaxFrameSize);
  }

  @Override
  public WhiteListInfoResp getWhiteIpSet()
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = (ISession) getSession();
      try {
        WhiteListInfoResp whiteListInfoResp = session.getWhiteIpSet();
        occupy(session);
        return whiteListInfoResp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("getWhiteIpSet failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return null;
  }

  @Override
  public void updateWhiteList(Set<String> ipSet)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = (ISession) getSession();
      try {
        session.updateWhiteList(ipSet);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("updateWhiteList failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  @Override
  public LicenseInfoResp getLicenseInfo()
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      ISession session = (ISession) getSession();
      try {
        LicenseInfoResp licenseInfoResp = session.getLicenseInfo();
        putBack(session);
        return licenseInfoResp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("getLicenseInfo failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    return null;
  }

  @Override
  protected ISession constructNewSession() {
    Session session;
    if (nodeUrls == null) {
      // Construct custom Session
      session =
          new Session.Builder()
              .host(host)
              .port(port)
              .username(user)
              .password(password)
              .fetchSize(fetchSize)
              .zoneId(zoneId)
              .thriftDefaultBufferSize(thriftDefaultBufferSize)
              .thriftMaxFrameSize(thriftMaxFrameSize)
              .enableRedirection(enableRedirection)
              .version(version)
              .build();
    } else {
      // Construct redirect-able Session
      session =
          new Session.Builder()
              .nodeUrls(nodeUrls)
              .username(user)
              .password(password)
              .fetchSize(fetchSize)
              .zoneId(zoneId)
              .thriftDefaultBufferSize(thriftDefaultBufferSize)
              .thriftMaxFrameSize(thriftMaxFrameSize)
              .enableRedirection(enableRedirection)
              .version(version)
              .build();
    }
    session.setEnableQueryRedirection(enableQueryRedirection);
    return session;
  }

  public static class Builder {

    private String host = SessionConfig.DEFAULT_HOST;
    private int port = SessionConfig.DEFAULT_PORT;
    private List<String> nodeUrls = null;
    private int maxSize = SessionConfig.DEFAULT_SESSION_POOL_MAX_SIZE;
    private String user = SessionConfig.DEFAULT_USER;
    private String password = SessionConfig.DEFAULT_PASSWORD;
    private int fetchSize = SessionConfig.DEFAULT_FETCH_SIZE;
    private long waitToGetSessionTimeoutInMs = 60_000;
    private int thriftDefaultBufferSize = SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY;
    private int thriftMaxFrameSize = SessionConfig.DEFAULT_MAX_FRAME_SIZE;
    private boolean enableCompression = false;
    private ZoneId zoneId = null;
    private boolean enableRedirection = SessionConfig.DEFAULT_REDIRECTION_MODE;
    private int connectionTimeoutInMs = SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS;
    private Version version = SessionConfig.DEFAULT_VERSION;
    private long timeOut = SessionConfig.DEFAULT_QUERY_TIME_OUT;

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder nodeUrls(List<String> nodeUrls) {
      this.nodeUrls = nodeUrls;
      return this;
    }

    public Builder maxSize(int maxSize) {
      this.maxSize = maxSize;
      return this;
    }

    public Builder user(String user) {
      this.user = user;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
      return this;
    }

    public Builder fetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    public Builder zoneId(ZoneId zoneId) {
      this.zoneId = zoneId;
      return this;
    }

    public Builder waitToGetSessionTimeoutInMs(long waitToGetSessionTimeoutInMs) {
      this.waitToGetSessionTimeoutInMs = waitToGetSessionTimeoutInMs;
      return this;
    }

    public Builder thriftDefaultBufferSize(int thriftDefaultBufferSize) {
      this.thriftDefaultBufferSize = thriftDefaultBufferSize;
      return this;
    }

    public Builder thriftMaxFrameSize(int thriftMaxFrameSize) {
      this.thriftMaxFrameSize = thriftMaxFrameSize;
      return this;
    }

    public Builder enableCompression(boolean enableCompression) {
      this.enableCompression = enableCompression;
      return this;
    }

    public Builder enableRedirection(boolean enableRedirection) {
      this.enableRedirection = enableRedirection;
      return this;
    }

    public Builder connectionTimeoutInMs(int connectionTimeoutInMs) {
      this.connectionTimeoutInMs = connectionTimeoutInMs;
      return this;
    }

    public Builder version(Version version) {
      this.version = version;
      return this;
    }

    public Builder timeOut(long timeOut) {
      this.timeOut = timeOut;
      return this;
    }

    public SessionPool build() {
      if (nodeUrls == null) {
        return new SessionPool(
            host,
            port,
            user,
            password,
            maxSize,
            fetchSize,
            waitToGetSessionTimeoutInMs,
            enableCompression,
            zoneId,
            enableRedirection,
            connectionTimeoutInMs,
            version,
            thriftDefaultBufferSize,
            thriftMaxFrameSize);
      } else {
        return new SessionPool(
            nodeUrls,
            user,
            password,
            maxSize,
            fetchSize,
            waitToGetSessionTimeoutInMs,
            enableCompression,
            zoneId,
            enableRedirection,
            connectionTimeoutInMs,
            version,
            thriftDefaultBufferSize,
            thriftMaxFrameSize);
      }
    }
  }
}
