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

package org.apache.iotdb.confignode.manager.pipe;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.mpp.rpc.thrift.TPipeOnDataNodeRegisterReq;
import org.apache.iotdb.mpp.rpc.thrift.TPipeOnLeaderChangeReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PipeRuntimeCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeRuntimeCoordinator.class);

  private final HeartbeatScheduler heartbeatScheduler = new HeartbeatScheduler();
  private final MetaSyncListener metaSyncListener = new MetaSyncListener();

  private final ConfigManager configManager;

  public PipeRuntimeCoordinator(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public HeartbeatScheduler getHeartbeatScheduler() {
    return heartbeatScheduler;
  }

  public MetaSyncListener getMetaSyncListener() {
    return metaSyncListener;
  }

  public void onLeaderChange(TConsensusGroupId regionGroupId, int oldLeader, int newLeader) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    final TPipeOnLeaderChangeReq request =
        new TPipeOnLeaderChangeReq(regionGroupId, oldLeader, newLeader);

    final AsyncClientHandler<TPipeOnLeaderChangeReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.PIPE_ON_LEADER_CHANGE, request, dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    if (RpcUtils.squashResponseStatusList(clientHandler.getResponseList()).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Execute pipeOnLeaderChange failed, req: {}", request);
    }
  }

  public void onDataNodeRegister(TEndPoint registerEndPoint) {
    TPipeOnDataNodeRegisterReq req = new TPipeOnDataNodeRegisterReq();
    // TODO: set pipeMetas
    SyncDataNodeClientPool.getInstance()
        .sendSyncRequestToDataNodeWithRetry(
            registerEndPoint, req, DataNodeRequestType.PIPE_ON_DATANODE_REGISTER);
  }

  public void onDataNodeRemove(TEndPoint removeEndPoint) {
    SyncDataNodeClientPool.getInstance()
        .sendSyncRequestToDataNodeWithRetry(
            removeEndPoint, null, DataNodeRequestType.PIPE_ON_DATANODE_REMOVE);
  }
}
