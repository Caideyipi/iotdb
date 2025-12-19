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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TTableDeviceLeaderReq;
import org.apache.iotdb.service.rpc.thrift.TTableDeviceLeaderResp;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionConnection;

import com.timecho.iotdb.isession.ITableSession;

import java.util.List;

public class TableSession extends org.apache.iotdb.session.TableSession implements ITableSession {

  TableSession(Session session) {
    super(session);
  }

  @Override
  public String getDeviceLeaderURL(
      String dbName, List<String> deviceId, List<Boolean> isSetTag, long time)
      throws IoTDBConnectionException, StatementExecutionException {
    return getDeviceLeaderURL(dbName, deviceId, isSetTag, time, session);
  }

  public static String getDeviceLeaderURL(
      String dbName, List<String> deviceId, List<Boolean> isSetTag, long time, Session session)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionConnection sessionConnection = session.getQuerySessionConnection();
    IClientRPCService.Iface client = sessionConnection.getClient();
    TTableDeviceLeaderReq req = new TTableDeviceLeaderReq(dbName, deviceId, isSetTag, time);
    final TTableDeviceLeaderResp resp =
        sessionConnection
            .callWithRetryAndReconnect(
                () -> client.fetchDeviceLeader(req), TTableDeviceLeaderResp::getStatus)
            .getResult();
    RpcUtils.verifySuccess(resp.getStatus());
    return resp.getIp() + ":" + resp.getPort();
  }
}
