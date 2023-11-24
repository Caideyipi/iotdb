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

/**
 * This class provides a license which has no restraint. Only for integration tests (except
 * IoTDBActivationTest)
 */
public class LicenseWithoutLimit extends License {
  public LicenseWithoutLimit(Runnable onLicenseChange) {
    super(onLicenseChange);
    this.licenseIssueTimestamp = 10000;
    // 4102416000000 ms == 2100-01-01 00:00:00
    this.licenseExpireTimestamp = 4102416000000L;
    this.dataNodeNumLimit = 9999;
    this.dataNodeCpuCoreNumLimit = 999999999;
    this.deviceNumLimit = Long.MAX_VALUE;
    this.sensorNumLimit = Long.MAX_VALUE;
    this.disconnectionFromActiveNodeTimeLimit = Long.MAX_VALUE;
    this.licenseSource = LicenseSource.FROM_FILE;
    this.aiNodeNumLimit = 9999;
  }
}
