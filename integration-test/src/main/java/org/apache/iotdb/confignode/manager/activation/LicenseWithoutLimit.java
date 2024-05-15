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

package org.apache.iotdb.confignode.manager.activation;

import com.timecho.iotdb.manager.activation.License;

/**
 * This class provides a license which has no restraint. Only for integration tests (except
 * IoTDBActivationTest)
 */
public class LicenseWithoutLimit extends License {
  private static long LICENSE_ISSUE_TIMESTAMP_UNLIMITED = 10000;
  private static long LICENSE_EXPIRE_TIMESTAMP_UNLIMITED =
      4102416000000L; // 4102416000000 == 2100-01-01 00:00:00
  private static short DATA_NODE_NUM_UNLIMITED = Short.MAX_VALUE;
  private static int DATA_NODE_CPU_CORE_NUM_UNLIMITED = Integer.MAX_VALUE;
  static long DEVICE_NUM_UNLIMITED = Long.MAX_VALUE;
  static long SENSOR_NUM_UNLIMITED = Long.MAX_VALUE;
  private static long DISCONNECTION_FROM_ACTIVE_NODE_TIME_UNLIMITED = Long.MAX_VALUE;
  private static LicenseSource LICENSE_SOURCE_UNLIMITED = LicenseSource.FROM_FILE;
  private static short AI_NODE_NUM_UNLIMITED = Short.MAX_VALUE;

  public LicenseWithoutLimit(Runnable onLicenseChange) {
    super(onLicenseChange);
    this.licenseIssueTimestamp = LICENSE_ISSUE_TIMESTAMP_UNLIMITED;
    this.licenseExpireTimestamp = LICENSE_EXPIRE_TIMESTAMP_UNLIMITED;
    this.dataNodeNumLimit = DATA_NODE_NUM_UNLIMITED;
    this.dataNodeCpuCoreNumLimit = DATA_NODE_CPU_CORE_NUM_UNLIMITED;
    this.deviceNumLimit = DEVICE_NUM_UNLIMITED;
    this.sensorNumLimit = SENSOR_NUM_UNLIMITED;
    this.disconnectionFromActiveNodeTimeLimit = DISCONNECTION_FROM_ACTIVE_NODE_TIME_UNLIMITED;
    this.licenseSource = LICENSE_SOURCE_UNLIMITED;
    this.aiNodeNumLimit = AI_NODE_NUM_UNLIMITED;
    this.onLicenseChange.run();
  }
}
