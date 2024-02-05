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

package org.apache.iotdb.it.env.cluster.env;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import com.timecho.iotdb.isession.ISession;
import com.timecho.iotdb.isession.pool.ISessionPool;
import com.timecho.iotdb.session.Session;
import com.timecho.iotdb.session.pool.SessionPool;

import java.util.*;

public class TimechoEnv extends SimpleEnv {
  @Override
  public ISession getSessionConnection() throws IoTDBConnectionException {
    DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    Session session = new Session(dataNode.getIp(), dataNode.getPort());
    session.open();
    return session;
  }

  @Override
  public ISession getSessionConnection(String userName, String password)
      throws IoTDBConnectionException {
    DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    Session session = new Session(dataNode.getIp(), dataNode.getPort(), userName, password);
    session.open();
    return session;
  }

  @Override
  public ISession getSessionConnection(List<String> nodeUrls) throws IoTDBConnectionException {
    Session session =
        new Session(
            nodeUrls,
            SessionConfig.DEFAULT_USER,
            SessionConfig.DEFAULT_PASSWORD,
            SessionConfig.DEFAULT_FETCH_SIZE,
            null,
            SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
            SessionConfig.DEFAULT_MAX_FRAME_SIZE,
            SessionConfig.DEFAULT_REDIRECTION_MODE,
            SessionConfig.DEFAULT_VERSION);
    session.open();
    return session;
  }

  @Override
  public ISessionPool getSessionPool(int maxSize) {
    DataNodeWrapper dataNode =
        this.dataNodeWrapperList.get(rand.nextInt(this.dataNodeWrapperList.size()));
    return new SessionPool(
        dataNode.getIp(),
        dataNode.getPort(),
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        maxSize);
  }
}
