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

import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.rpc.thrift.TAINodeRegisterReq;
import org.apache.iotdb.consensus.common.DataSet;

import com.timecho.iotdb.manager.load.TimechoLoadManager;
import com.timecho.iotdb.manager.node.TimechoNodeManager;
import com.timecho.iotdb.manager.regulate.RegulateManager;

public interface ITimechoManager extends IManager {
  /**
   * Get ActivationManager
   *
   * @return ActivationManager instance
   */
  RegulateManager getActivationManager();

  /**
   * Register AINode
   *
   * @param req TAINodeRegisterReq
   * @return AINodeConfigurationDataSet
   */
  DataSet registerAINode(TAINodeRegisterReq req);

  @Override
  TimechoLoadManager getLoadManager();

  @Override
  TimechoNodeManager getNodeManager();
}
