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

package org.apache.iotdb.confignode.manager.load.cache.node;

import org.apache.iotdb.commons.license.ActivateStatus;

public class ActivationStatusCache {
  // After expireTime, active node will be treated as disconnected.
  public static final long EXPIRE_TIMEOUT = BaseNodeCache.HEARTBEAT_TIMEOUT_TIME;
  private final long timestamp;
  private final ActivateStatus activateStatus;

  public ActivationStatusCache() {
    this.timestamp = 0;
    this.activateStatus = ActivateStatus.UNKNOWN;
  }

  public ActivationStatusCache(long timestamp, ActivateStatus activateStatus) {
    this.timestamp = timestamp;
    this.activateStatus = activateStatus;
  }

  public ActivateStatus getActivateStatus() {
    return activateStatus;
  }

  public boolean isActive() {
    return activateStatus.isActive();
  }

  public boolean tooOld() {
    return System.currentTimeMillis() - timestamp > EXPIRE_TIMEOUT;
  }
}
