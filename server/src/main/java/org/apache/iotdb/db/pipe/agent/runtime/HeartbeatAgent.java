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

package org.apache.iotdb.db.pipe.agent.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.mpp.rpc.thrift.TPipeTaskHeartbeat;

import java.util.HashMap;
import java.util.Map;

/** HeartbeatAgent is used to handle the heartbeat collection of the pipe. */
public class HeartbeatAgent {
  public Map<String, Map<TConsensusGroupId, TPipeTaskHeartbeat>> collectPipeHeartbeat() {
    // TODO: collect the pipe heartbeat for the existing pipes
    return new HashMap<>();
  }
}
