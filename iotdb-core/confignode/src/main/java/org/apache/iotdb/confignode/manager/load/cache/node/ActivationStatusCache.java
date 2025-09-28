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

import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import com.timecho.iotdb.commons.commission.obligation.ObligationStatus;

public class ActivationStatusCache {
  // After expireTime, active node will be treated as disconnected.
  public static final long EXPIRE_TIMEOUT =
      ConfigNodeDescriptor.getInstance().getConf().getFailureDetectorFixedThresholdInMs()
          * 1000_000L;
  private boolean fake = false;
  private final long timestamp;
  private final ObligationStatus activateStatus;

  public ActivationStatusCache() {
    this.fake = true;
    this.timestamp = 0;
    this.activateStatus = ObligationStatus.UNKNOWN;
  }

  public ActivationStatusCache(long timestamp, ObligationStatus activateStatus) {
    this.timestamp = timestamp;
    this.activateStatus = activateStatus;
  }

  public ObligationStatus getActivateStatus() {
    return activateStatus;
  }

  /**
   * When an ActivationStatusCache is fake, it indicates that this cache does not come from a real
   * heartbeat but is just used as a placeholder.
   */
  public boolean isFake() {
    return fake;
  }

  public boolean isActive() {
    return activateStatus.isActive();
  }

  public boolean tooOld() {
    return System.nanoTime() - timestamp > EXPIRE_TIMEOUT;
  }
}
