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

package org.apache.iotdb.confignode.procedure.impl.node;

import org.apache.iotdb.common.rpc.thrift.TMLNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.write.mlnode.RemoveMLNodePlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.state.RemoveMLNodeState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RemoveMLNodeProcedure extends AbstractNodeProcedure<RemoveMLNodeState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveMLNodeProcedure.class);
  private static final int RETRY_THRESHOLD = 5;

  private TMLNodeLocation removedMLNode;

  public RemoveMLNodeProcedure(TMLNodeLocation removedMLNode) {
    super();
    this.removedMLNode = removedMLNode;
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveMLNodeState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (removedMLNode == null) {
      return Flow.NO_MORE_STATE;
    }

    try {
      switch (state) {
        case MODEL_DELETE:
          setNextState(RemoveMLNodeState.NODE_REMOVE);
          break;
        case NODE_REMOVE:
          TSStatus response =
              env.getConfigManager()
                  .getConsensusManager()
                  .write(new RemoveMLNodePlan(removedMLNode));

          if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            throw new ProcedureException(
                String.format(
                    "Fail to remove [%s] MLNode on Config Nodes [%s]",
                    removedMLNode, response.getMessage()));
          }
          setNextState(RemoveMLNodeState.NODE_REMOVE);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(
              String.format("Unknown state during executing removeMLNodeProcedure, %s", state));
      }
    } catch (Exception e) {
      if (isRollbackSupported(state)) {
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error(
            "Retrievable error trying to remove MLNode [{}], state [{}]", removedMLNode, state, e);
        if (getCycles() > RETRY_THRESHOLD) {
          setFailure(
              new ProcedureException(
                  String.format(
                      "Fail to remove MLNode [%s] at STATE [%s], %s",
                      removedMLNode, state, e.getMessage())));
        }
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, RemoveMLNodeState removeMLNodeState)
      throws IOException, InterruptedException, ProcedureException {
    // no need to rollback
  }

  @Override
  protected RemoveMLNodeState getState(int stateId) {
    return RemoveMLNodeState.values()[stateId];
  }

  @Override
  protected int getStateId(RemoveMLNodeState removeMLNodeState) {
    return removeMLNodeState.ordinal();
  }

  @Override
  protected RemoveMLNodeState getInitialState() {
    return RemoveMLNodeState.MODEL_DELETE;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.REMOVE_ML_NODE_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTMLNodeLocation(removedMLNode, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    removedMLNode = ThriftCommonsSerDeUtils.deserializeTMLNodeLocation(byteBuffer);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof RemoveMLNodeProcedure) {
      RemoveMLNodeProcedure thatProc = (RemoveMLNodeProcedure) that;
      return thatProc.getProcId() == this.getProcId()
          && thatProc.getState() == this.getState()
          && (thatProc.removedMLNode).equals(this.removedMLNode);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getProcId(), getState(), removedMLNode);
  }
}
