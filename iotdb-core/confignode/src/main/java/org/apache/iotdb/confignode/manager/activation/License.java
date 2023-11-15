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

import org.apache.iotdb.common.rpc.thrift.TLicense;
import org.apache.iotdb.commons.exception.LicenseException;
import org.apache.iotdb.commons.license.ActivateStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.Properties;

public class License {
  private static final Logger logger = LoggerFactory.getLogger(License.class);

  // license common fields
  public static final String LICENSE_ISSUE_TIMESTAMP_NAME = "L1";
  public static final String LICENSE_EXPIRE_TIMESTAMP_NAME = "L2";
  public static final String DISCONNECTION_FROM_ACTIVE_NODE_TIME_LIMIT_NAME = "L3";

  // system info fields
  public static final String CPU_ID_NAME = "S1";
  public static final String MAIN_BOARD_ID_NAME = "S2";
  public static final String SYSTEM_UUID_NAME = "S3";
  public static final String IP_ADDRESS_NAME = "S4";
  public static final String INTERNAL_PORT_NAME = "S5";
  public static final String IS_SEED_CONFIGNODE_NODE_NAME = "S6";
  public static final String CLUSTER_NAME_NAME = "S7";

  // DataNode fields
  public static final String DATANODE_NUM_LIMIT_NAME = "DN1";
  public static final String DATANODE_CPU_CORE_NUM_LIMIT_NAME = "DN2";
  public static final String DEVICE_NUM_LIMIT_NAME = "DN3";
  public static final String SENSOR_NUM_LIMIT_NAME = "DN4";

  // MLNode fields
  public static final String MLNODE_NUM_LIMIT_NAME = "ML1";

  // activate info
  protected long licenseIssueTimestamp = 0;
  protected long licenseExpireTimestamp = 0;
  protected short dataNodeNumLimit = 0;
  protected int dataNodeCpuCoreNumLimit = 0;
  protected long deviceNumLimit = 0;
  protected long sensorNumLimit = 0;
  protected long disconnectionFromActiveNodeTimeLimit = 0;
  protected short mlNodeNumLimit = 0;

  // other info
  protected enum LicenseSource {
    FROM_FILE,
    FROM_REMOTE,
    UNKNOWN
  }

  protected LicenseSource licenseSource = LicenseSource.UNKNOWN;

  private final Runnable onLicenseChange;

  // endregion

  public License(Runnable onLicenseChange) {
    this.onLicenseChange = onLicenseChange;
  }

  // region getter and setter

  public long getLicenseExpireTimestamp() {
    return licenseExpireTimestamp;
  }

  public long getLicenseIssueTimestamp() {
    return licenseIssueTimestamp;
  }

  public short getDataNodeNumLimit() {
    return dataNodeNumLimit;
  }

  public int getDataNodeCpuCoreNumLimit() {
    return dataNodeCpuCoreNumLimit;
  }

  public long getDeviceNumLimit() {
    return deviceNumLimit;
  }

  public long getSensorNumLimit() {
    return sensorNumLimit;
  }

  public long getDisconnectionFromActiveNodeTimeLimit() {
    return this.disconnectionFromActiveNodeTimeLimit;
  }

  public short getMLNodeNumLimit() {
    return this.mlNodeNumLimit;
  }

  // endregion

  public void reset() {
    // activate info
    licenseIssueTimestamp = 0;
    licenseExpireTimestamp = 0;
    dataNodeNumLimit = 0;
    dataNodeCpuCoreNumLimit = 0;
    deviceNumLimit = 0;
    sensorNumLimit = 0;
    disconnectionFromActiveNodeTimeLimit = 0;
    mlNodeNumLimit = 0;
    // other
    licenseSource = LicenseSource.UNKNOWN;
  }

  // region load method

  public void loadFromProperties(Properties properties) {
    // try load properties
    License newLicense = new License(null);
    // activate info
    try {
      newLicense.licenseIssueTimestamp = getLong(LICENSE_ISSUE_TIMESTAMP_NAME, properties);
      newLicense.licenseExpireTimestamp = getLong(LICENSE_EXPIRE_TIMESTAMP_NAME, properties);
      newLicense.dataNodeNumLimit = (short) getLong(DATANODE_NUM_LIMIT_NAME, properties);
      newLicense.dataNodeCpuCoreNumLimit =
          (int) getLong(DATANODE_CPU_CORE_NUM_LIMIT_NAME, properties);
      newLicense.deviceNumLimit = getLong(DEVICE_NUM_LIMIT_NAME, properties);
      newLicense.sensorNumLimit = getLong(SENSOR_NUM_LIMIT_NAME, properties);
      newLicense.disconnectionFromActiveNodeTimeLimit =
          getLong(DISCONNECTION_FROM_ACTIVE_NODE_TIME_LIMIT_NAME, properties);
      newLicense.mlNodeNumLimit = (short) getLong(MLNODE_NUM_LIMIT_NAME, properties);
    } catch (LicenseException | NumberFormatException e) {
      logger.error("License parse error", e);
      return;
    }

    ActivateStatus oldActivateStatus = this.getActivateStatus();

    // compare and copy
    this.logLicenseDifferences(newLicense);
    this.copyFrom(newLicense);
    this.licenseSource = LicenseSource.FROM_FILE;

    // if activate status change, log
    if (oldActivateStatus != this.getActivateStatus()) {
      showActivateStatusChange(oldActivateStatus);
    }

    this.onLicenseChange.run();
  }

  public void loadFromTLicense(TLicense license) {
    License newLicense = new License(null);
    newLicense.licenseIssueTimestamp = license.licenseIssueTimestamp;
    newLicense.licenseExpireTimestamp = license.getExpireTimestamp();
    newLicense.dataNodeCpuCoreNumLimit = license.cpuCoreNumLimit;
    newLicense.dataNodeNumLimit = license.dataNodeNumLimit;
    newLicense.deviceNumLimit = license.deviceNumLimit;
    newLicense.sensorNumLimit = license.sensorNumLimit;
    newLicense.disconnectionFromActiveNodeTimeLimit =
        license.getDisconnectionFromActiveNodeTimeLimit();
    newLicense.mlNodeNumLimit = license.getMlNodeNumLimit();

    ActivateStatus oldActivateStatus = this.getActivateStatus();

    // compare and copy
    this.logLicenseDifferences(newLicense);
    this.copyFrom(newLicense);
    this.licenseSource = LicenseSource.FROM_REMOTE;

    // if activate status change, log
    if (oldActivateStatus != this.getActivateStatus()) {
      showActivateStatusChange(oldActivateStatus);
    }

    this.onLicenseChange.run();
  }

  // endregion

  // region helper method

  private long getLong(String str, Properties properties) throws LicenseException {
    if (!properties.containsKey(str)) {
      throw new LicenseException(
          String.format("%s is necessary, but cannot be found in license file", str));
    }
    try {
      return Long.parseLong(properties.getProperty(str).trim());
    } catch (NumberFormatException e) {
      throw new LicenseException(str + " field cannot be parsed to Long", e);
    }
  }

  private void showActivateStatusChange(ActivateStatus oldActivateStatus) {
    logger.info(
        "ConfigNode activate status change: {} -> {}", oldActivateStatus, this.getActivateStatus());
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long nowTimestamp = System.currentTimeMillis();
    String expireTimeString = dateFormat.format(licenseExpireTimestamp);
    String nowTimeString = dateFormat.format(nowTimestamp);
    logger.info(
        "<expireTime {}> {} <now {}>",
        expireTimeString,
        licenseExpireTimestamp > nowTimestamp ? ">" : " is earlier than ",
        nowTimeString);
  }

  private void logFieldDifference(String name, Object mine, Object another) {
    if (!Objects.equals(mine, another)) {
      logger.info("{}: {} -> {}", name, mine, another);
    }
  }

  private void logLicenseDifferences(License anotherLicense) {
    logFieldDifference(
        "licenseIssueTimestamp", this.licenseIssueTimestamp, anotherLicense.licenseIssueTimestamp);
    logFieldDifference(
        "licenseExpireTimestamp",
        this.licenseExpireTimestamp,
        anotherLicense.licenseExpireTimestamp);
    logFieldDifference("dataNodeNumLimit", this.dataNodeNumLimit, anotherLicense.dataNodeNumLimit);
    logFieldDifference(
        "dataNodeCpuCoreNumLimit",
        this.dataNodeCpuCoreNumLimit,
        anotherLicense.dataNodeCpuCoreNumLimit);
    logFieldDifference("deviceNumLimit", this.deviceNumLimit, anotherLicense.deviceNumLimit);
    logFieldDifference("sensorNumLimit", this.sensorNumLimit, anotherLicense.sensorNumLimit);
    logFieldDifference(
        "disconnectionFromActiveNodeTimeLimit",
        this.disconnectionFromActiveNodeTimeLimit,
        anotherLicense.disconnectionFromActiveNodeTimeLimit);
    logFieldDifference("mlNodeNumLimit", this.mlNodeNumLimit, anotherLicense.mlNodeNumLimit);
  }

  // show difference between old license and new license
  private void copyFrom(License anotherLicense) {
    this.licenseIssueTimestamp = anotherLicense.licenseIssueTimestamp;
    this.licenseExpireTimestamp = anotherLicense.licenseExpireTimestamp;
    this.dataNodeNumLimit = anotherLicense.dataNodeNumLimit;
    this.dataNodeCpuCoreNumLimit = anotherLicense.dataNodeCpuCoreNumLimit;
    this.deviceNumLimit = anotherLicense.deviceNumLimit;
    this.sensorNumLimit = anotherLicense.sensorNumLimit;
    this.disconnectionFromActiveNodeTimeLimit = anotherLicense.disconnectionFromActiveNodeTimeLimit;
    this.mlNodeNumLimit = anotherLicense.mlNodeNumLimit;
  }

  public TLicense toTLicense() {
    return new TLicense(
        this.getLicenseIssueTimestamp(),
        this.getLicenseExpireTimestamp(),
        this.getDataNodeNumLimit(),
        this.getDataNodeCpuCoreNumLimit(),
        this.getDeviceNumLimit(),
        this.getSensorNumLimit(),
        this.getDisconnectionFromActiveNodeTimeLimit(),
        this.getMLNodeNumLimit());
  }

  // endregion

  // region show status to outside

  public boolean isActivated() {
    return this.getLicenseExpireTimestamp() >= System.currentTimeMillis();
  }

  public boolean isActive() {
    return LicenseSource.FROM_FILE.equals(licenseSource);
  }

  /** Only used when detected license file deletion */
  public void licenseFileNotExistOrInvalid() {
    licenseSource = LicenseSource.UNKNOWN;
  }

  public ActivateStatus getActivateStatus() {
    if (this.isActive()) {
      if (this.isActivated()) {
        return ActivateStatus.ACTIVE_ACTIVATED;
      } else {
        return ActivateStatus.ACTIVE_UNACTIVATED;
      }
    } else {
      if (this.isActivated()) {
        return ActivateStatus.PASSIVE_ACTIVATED;
      } else {
        return ActivateStatus.PASSIVE_UNACTIVATED;
      }
    }
  }
  // endregion
}
