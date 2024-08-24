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

package com.timecho.iotdb.manager;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.exception.LicenseException;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TNodeActivateInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;

import com.timecho.iotdb.manager.activation.ActivationManager;
import com.timecho.iotdb.manager.load.TimechoLoadManager;
import com.timecho.iotdb.manager.node.TimechoNodeManager;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TimechoConfigManager extends org.apache.iotdb.confignode.manager.ConfigManager
    implements ITimechoManager {
  private TimechoNodeManager timechoNodeManager;
  private TimechoLoadManager timechoLoadManager;
  protected ActivationManager activationManager;

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  public TimechoConfigManager() throws IOException, LicenseException {
    super();
    initActivationManager();
  }

  @Override
  protected void setNodeManager(NodeInfo nodeInfo) {
    this.timechoNodeManager = new TimechoNodeManager(this, nodeInfo);
    super.nodeManager = this.timechoNodeManager;
  }

  @Override
  public TimechoNodeManager getNodeManager() {
    return this.timechoNodeManager;
  }

  @Override
  protected void setLoadManager() {
    this.timechoLoadManager = new TimechoLoadManager(this);
    super.loadManager = this.timechoLoadManager;
  }

  @Override
  public TimechoLoadManager getLoadManager() {
    return timechoLoadManager;
  }

  @Override
  public ActivationManager getActivationManager() {
    return activationManager;
  }

  @Override
  public TShowClusterResp showCluster() {
    TShowClusterResp result = super.showCluster();

    List<TAINodeLocation> aiNodeInfoLocations =
        timechoNodeManager.getRegisteredAINodes().stream()
            .map(TAINodeConfiguration::getLocation)
            .sorted(Comparator.comparingInt(TAINodeLocation::getAiNodeId))
            .collect(Collectors.toList());
    Map<Integer, String> nodeStatusMap = getLoadManager().getNodeStatusWithReason();
    aiNodeInfoLocations.forEach(
        aiNodeLocation ->
            nodeStatusMap.putIfAbsent(aiNodeLocation.getAiNodeId(), NodeStatus.Unknown.toString()));
    result.setAiNodeList(aiNodeInfoLocations);

    Map<Integer, TNodeActivateInfo> nodeActivateInfoMap =
        getLoadManager().getNodeSimplifiedActivateStatus();
    nodeActivateInfoMap.put(
        CONF.getConfigNodeId(),
        new TNodeActivateInfo(activationManager.getActivateStatus().toSimpleString()));
    result.setNodeActivateInfo(nodeActivateInfoMap);

    return result;
  }

  protected void initActivationManager() throws LicenseException {
    this.activationManager = new ActivationManager(this);
  }
}
