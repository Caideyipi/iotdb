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

package org.apache.iotdb.confignode.procedure.impl.sync;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.consensus.request.write.auth.EnableSeparationOfAdminPowersPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.auth.EnableSeparationOfAdminPowersProcedureState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.iotdb.confignode.procedure.state.auth.EnableSeparationOfAdminPowersProcedureState.INIT;
import static org.apache.iotdb.confignode.procedure.state.auth.EnableSeparationOfAdminPowersProcedureState.UPDATE_DATANODE_STATUS;

public class EnableSeparationOfAdminPowersProcedure
    extends AbstractNodeProcedure<EnableSeparationOfAdminPowersProcedureState> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(EnableSeparationOfAdminPowersProcedure.class);

  private EnableSeparationOfAdminPowersPlan plan;

  public EnableSeparationOfAdminPowersProcedure() {}

  public EnableSeparationOfAdminPowersProcedure(EnableSeparationOfAdminPowersPlan plan) {
    this.plan = plan;
  }

  @Override
  protected Flow executeFromState(
      ConfigNodeProcedureEnv configNodeProcedureEnv,
      EnableSeparationOfAdminPowersProcedureState enableSeparationOfAdminPowersProcedureState)
      throws InterruptedException {
    switch (enableSeparationOfAdminPowersProcedureState) {
      case INIT:
        writePlan(configNodeProcedureEnv);
        return Flow.HAS_MORE_STATE;
      case UPDATE_DATANODE_STATUS:
        updateDatanodeStatus(configNodeProcedureEnv);
        return Flow.NO_MORE_STATE;
    }
    return Flow.HAS_MORE_STATE;
  }

  private void writePlan(ConfigNodeProcedureEnv env) {
    TSStatus status =
        env.getConfigManager()
            .getPermissionManager()
            .enableSeparationOfPowers(
                plan.getSystemAdminUsername(),
                plan.getSecurityAdminUsername(),
                plan.getAuditAdminUsername());
    if (status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      setNextState(UPDATE_DATANODE_STATUS);
    } else {
      LOGGER.info("Failed to execute plan {} because {}", plan, status.message);
      setFailure(new ProcedureException(new IoTDBException(status)));
    }
  }

  private void updateDatanodeStatus(ConfigNodeProcedureEnv env) {
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        env.getConfigManager().getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<Object, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.ENABLE_SEPARATION_OF_ADMIN_POWERS, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    if (RpcUtils.squashResponseStatusList(clientHandler.getResponseList()).getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Failed to update datanode status");
    }
  }

  @Override
  protected boolean isRollbackSupported(
      EnableSeparationOfAdminPowersProcedureState enableSeparationOfAdminPowersProcedureState) {
    return enableSeparationOfAdminPowersProcedureState == INIT;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv,
      EnableSeparationOfAdminPowersProcedureState enableSeparationOfAdminPowersProcedureState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected EnableSeparationOfAdminPowersProcedureState getState(int stateId) {
    return EnableSeparationOfAdminPowersProcedureState.values()[stateId];
  }

  @Override
  protected int getStateId(
      EnableSeparationOfAdminPowersProcedureState enableSeparationOfAdminPowersProcedureState) {
    return enableSeparationOfAdminPowersProcedureState.ordinal();
  }

  @Override
  protected EnableSeparationOfAdminPowersProcedureState getInitialState() {
    return INIT;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.ENABLE_SEPARATION_OF_ADMIN_POWERS_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ReadWriteIOUtils.write(plan.getSystemAdminUsername(), stream);
    ReadWriteIOUtils.write(plan.getSecurityAdminUsername(), stream);
    ReadWriteIOUtils.write(plan.getAuditAdminUsername(), stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    String systemAdminUsername = ReadWriteIOUtils.readString(byteBuffer);
    String securityAdminUsername = ReadWriteIOUtils.readString(byteBuffer);
    String auditAdminUsername = ReadWriteIOUtils.readString(byteBuffer);
    plan =
        new EnableSeparationOfAdminPowersPlan(
            systemAdminUsername, securityAdminUsername, auditAdminUsername);
  }
}
