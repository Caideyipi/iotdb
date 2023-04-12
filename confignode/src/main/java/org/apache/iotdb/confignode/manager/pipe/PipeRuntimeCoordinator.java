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
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

public class PipeRuntimeCoordinator {
  private final HeartbeatScheduler heartbeatScheduler = new HeartbeatScheduler();
  private final MetaSyncListener metaSyncListener = new MetaSyncListener();

  public PipeRuntimeCoordinator() {}

  public TSStatus onLeaderChange(TConsensusGroupId regionGroupId, int oldLeader, int newLeader) {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus onDataNodeRegister(TEndPoint registerEndPoint) {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus onDataNodeRemove(TEndPoint removeEndPoint) {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
