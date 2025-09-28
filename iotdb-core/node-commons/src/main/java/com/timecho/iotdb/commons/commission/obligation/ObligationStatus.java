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

package com.timecho.iotdb.commons.commission.obligation;

public enum ObligationStatus {
  // For ConfigNode
  ACTIVE_ACTIVATED("ACTIVE_ACTIVATED"),
  PASSIVE_ACTIVATED("PASSIVE_ACTIVATED"),
  ACTIVE_UNACTIVATED("ACTIVE_UNACTIVATED"),
  PASSIVE_UNACTIVATED("PASSIVE_UNACTIVATED"),
  // For DataNode
  ACTIVATED("ACTIVATED"),
  UNACTIVATED("UNACTIVATED"),
  // For ConfigNode and DataNode
  UNKNOWN("UNKNOWN"),
  // For cluster status
  PARTLY_ACTIVATED("PARTLY_ACTIVATED");

  private final String status;

  ObligationStatus(String status) {
    this.status = status;
  }

  public boolean isActive() {
    return ACTIVE_ACTIVATED.equals(this) || ACTIVE_UNACTIVATED.equals(this);
  }

  public boolean isFullyActivated() {
    return ACTIVE_ACTIVATED.equals(this) || ACTIVATED.equals(this);
  }

  public boolean isActivated() {
    return ACTIVE_ACTIVATED.equals(this)
        || PASSIVE_ACTIVATED.equals(this)
        || ACTIVATED.equals(this);
  }

  public boolean isUnactivated() {
    return ACTIVE_UNACTIVATED.equals(this)
        || PASSIVE_UNACTIVATED.equals(this)
        || UNACTIVATED.equals(this);
  }

  @Override
  public String toString() {
    return this.status;
  }

  public String toSimpleString() {
    switch (this) {
      case ACTIVE_ACTIVATED:
      case ACTIVATED:
        return "ACTIVATED";
      case PASSIVE_ACTIVATED:
        return "ACTIVATED(W)";
      case ACTIVE_UNACTIVATED:
      case PASSIVE_UNACTIVATED:
      case UNACTIVATED:
        return "UNACTIVATED";
      case UNKNOWN:
        return UNKNOWN.toString();
      default:
        throw new UnsupportedOperationException(
            String.format("toSimpleString() not adapted %s yet", this));
    }
  }
}
