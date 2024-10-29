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

package com.timecho.iotdb.manager.load;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.node.ActivationStatusCache;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TNodeActivateInfo;

import com.timecho.iotdb.manager.ITimechoManager;
import com.timecho.iotdb.manager.load.service.TimechoHeartbeatService;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TimechoLoadManager extends LoadManager {

  private final ITimechoManager timechoConfigManager;

  public TimechoLoadManager(ITimechoManager configManager) {
    super(configManager);
    timechoConfigManager = configManager;
  }

  @Override
  protected void setHeartbeatService(IManager configManager, LoadCache loadCache) {
    heartbeatService = new TimechoHeartbeatService((ITimechoManager) configManager, loadCache);
  }

  /**
   * Get all Node's current activation info
   *
   * @return Map<NodeId, Node activation info
   */
  public Map<Integer, TNodeActivateInfo> getNodeSimplifiedActivateStatus() {
    return loadCache.getNodeSimplifiedActivateStatus();
  }

  public Map<Integer, String> getNodeActivateStatus() {
    Map<Integer, String> result = loadCache.getNodeActivateStatus();
    result.put(
        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
        timechoConfigManager.getActivationManager().getActivateStatus().toString());
    return result;
  }

  /** Check if there is any active node keep living. */
  public boolean activeNodeLive() {
    return loadCache.getActivationStatusCacheMap().values().stream()
        .anyMatch(cache -> !cache.isFake() && !cache.tooOld() && cache.isActive());
  }

  /** Check if there is any active node disconnects. */
  public boolean activeNodeDisconnect() {
    return loadCache.getActivationStatusCacheMap().values().stream()
        .anyMatch(cache -> !cache.isFake() && cache.tooOld() && cache.isActive());
  }

  public boolean someConfigNodeNotSentHeartbeatYet() {
    return loadCache.getActivationStatusCacheMap().entrySet().stream()
        .anyMatch(
            entry ->
                entry.getKey() != ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()
                    && entry.getValue().isFake());
  }

  public void updateActivationStatusCache() {
    Set<Integer> configNodeIdSet =
        configManager.getNodeManager().getRegisteredConfigNodeInfoList().stream()
            .filter(
                info ->
                    !info.status.startsWith(
                        NodeStatus.Removing.getStatus())) // Not in Removing status
            .map(TConfigNodeInfo::getConfigNodeId)
            .collect(Collectors.toSet());
    // Put if absent
    configNodeIdSet.forEach(
        id -> loadCache.getActivationStatusCacheMap().putIfAbsent(id, new ActivationStatusCache()));
    // Remove if present
    for (Integer cnId : loadCache.getActivationStatusCacheMap().keySet()) {
      if (!configNodeIdSet.contains(cnId)) {
        loadCache.getActivationStatusCacheMap().remove(cnId);
      }
    }
  }
}
