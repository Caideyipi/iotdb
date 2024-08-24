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

package com.timecho.iotdb.service.thrift;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.rpc.thrift.TActivationControl;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllActivationStatusResp;
import org.apache.iotdb.confignode.rpc.thrift.TLicenseContentResp;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCServiceProcessor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.manager.TimechoConfigManager;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

public class TimechoConfigNodeRPCServiceProcessor extends ConfigNodeRPCServiceProcessor {
  private static Logger LOGGER =
      LoggerFactory.getLogger(TimechoConfigNodeRPCServiceProcessor.class);
  private final TimechoConfigManager configManager;

  public TimechoConfigNodeRPCServiceProcessor(TimechoConfigManager configManager) {
    super(configManager);
    this.configManager = configManager;
  }

  @Override
  public TConfigNodeHeartbeatResp getConfigNodeHeartBeat(TConfigNodeHeartbeatReq heartbeatReq) {
    TConfigNodeHeartbeatResp resp = new TConfigNodeHeartbeatResp();
    resp.setTimestamp(heartbeatReq.getTimestamp());
    if (heartbeatReq.isSetActivationControl()) {
      if (Objects.equals(
          heartbeatReq.getActivationControl(), TActivationControl.ALL_LICENSE_FILE_DELETED)) {
        configManager
            .getActivationManager()
            .giveUpLicenseBecauseLeaderBelieveThereIsNoActiveNodeInCluster();
      } else {
        throw new UnsupportedOperationException(
            String.format("%s is not supported", heartbeatReq.getActivationControl()));
      }
    }
    configManager.getActivationManager().tryLoadRemoteLicense(heartbeatReq.getLicence());
    resp.setActivateStatus(configManager.getActivationManager().getActivateStatus().toString());
    if (configManager.getActivationManager().isActive()) {
      // Report my license only if I'm active
      resp.setLicense(configManager.getActivationManager().getLicense().toTLicense());
    }
    return resp;
  }

  @Override
  public TSStatus setLicenseFile(String fileName, String content) throws TException {
    return configManager.getActivationManager().setLicenseFile(fileName, content);
  }

  @Override
  public TSStatus deleteLicenseFile(String fileName) throws TException {
    return configManager.getActivationManager().deleteLicenseFile(fileName);
  }

  @Override
  public TSStatus getLicenseFile(String fileName) throws TException {
    return configManager.getActivationManager().getLicenseFile(fileName);
  }

  @Override
  public TLicenseContentResp getLicenseContent() throws TException {
    TLicenseContentResp resp =
        new TLicenseContentResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    resp.setLicense(configManager.getActivationManager().getLicense().toTLicense());
    resp.setUsage(configManager.getActivationManager().getLicenseUsage());
    return resp;
  }

  @Override
  public TSStatus getActivateStatus() throws TException {
    TSStatus result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    result.setMessage(configManager.getActivationManager().getActivateStatus().toString());
    return result;
  }

  @TestOnly
  @Override
  public TGetAllActivationStatusResp getAllActivationStatus() throws TException {
    TGetAllActivationStatusResp resp = new TGetAllActivationStatusResp();
    resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    Map<Integer, String> activationMap = configManager.getLoadManager().getNodeActivateStatus();
    activationMap.put(
        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
        configManager.getActivationManager().getActivateStatus().toString());
    resp.setActivationStatusMap(activationMap);
    return resp;
  }
}
