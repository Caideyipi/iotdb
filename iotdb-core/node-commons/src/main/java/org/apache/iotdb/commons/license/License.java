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

package org.apache.iotdb.commons.license;

import org.apache.iotdb.common.rpc.thrift.TLicense;
import org.apache.iotdb.commons.exception.LicenseException;
import org.apache.iotdb.commons.license.limit.Limit;
import org.apache.iotdb.commons.license.limit.LimitAllowAbsent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class License {
  private static final Logger logger = LoggerFactory.getLogger(License.class);

  // license common fields
  public static final String LICENSE_ISSUE_TIMESTAMP_NAME = "L1";
  public static final String LICENSE_EXPIRE_TIMESTAMP_NAME = "L2";
  public static final String DISCONNECTION_FROM_ACTIVE_NODE_TIME_LIMIT_NAME = "L3";
  public static final String SKIP_HARDWARE_SYSTEM_INFO_CHECK_NAME = "L4";

  // system info fields
  public static final String CPU_ID_NAME = "S1";
  public static final String MAIN_BOARD_ID_NAME = "S2";
  public static final String SYSTEM_UUID_NAME = "S3";
  public static final String IP_ADDRESS_NAME = "S4";
  public static final String INTERNAL_PORT_NAME = "S5";
  public static final String IS_SEED_CONFIGNODE_NODE_NAME = "S6";
  public static final String CLUSTER_NAME_NAME = "S7";
  public static final String NODE_UUID_NAME = "S8";
  public static final String SYSTEM_INFO_HASH = "S9";

  // DataNode fields
  public static final String DATANODE_NUM_LIMIT_NAME = "DN1";
  public static final String DATANODE_CPU_CORE_NUM_LIMIT_NAME = "DN2";
  public static final String DEVICE_NUM_LIMIT_NAME = "DN3";
  public static final String SENSOR_NUM_LIMIT_NAME = "DN4";

  // AINode fields
  public static final String AINODE_NUM_LIMIT_NAME = "ML1";

  // activate info
  protected final Limit<Long> licenseIssueTimestamp = new Limit<>(0L, Long::parseLong);
  protected final Limit<Long> licenseExpireTimestamp = new Limit<>(0L, Long::parseLong);
  protected final Limit<Long> disconnectionFromActiveNodeTimeLimit =
      new LimitAllowAbsent<>(0L, Long::parseLong, TimeUnit.DAYS.toMillis(7));
  protected final Limit<Boolean> skipHardwareSystemInfoCheck =
      new Limit<>(false, Boolean::parseBoolean);
  protected final Limit<Short> dataNodeNumLimit =
      new LimitAllowAbsent<>((short) 0, Short::parseShort, Short.MAX_VALUE);
  protected final Limit<Integer> dataNodeCpuCoreNumLimit =
      new LimitAllowAbsent<>(0, Integer::parseInt, Integer.MAX_VALUE);
  protected final Limit<Long> deviceNumLimit =
      new LimitAllowAbsent<>(0L, Long::parseLong, Long.MAX_VALUE);
  protected final Limit<Long> sensorNumLimit =
      new LimitAllowAbsent<>(0L, Long::parseLong, Long.MAX_VALUE);
  protected final Limit<Short> aiNodeNumLimit =
      new LimitAllowAbsent<>((short) 0, Short::parseShort, (short) 0);
  private final List<Limit<?>> allLimit;

  // other info
  protected enum LicenseSource {
    FROM_FILE,
    FROM_REMOTE,
    UNKNOWN,
    NO_LICENSE
  }

  protected LicenseSource licenseSource = LicenseSource.UNKNOWN;

  protected final Runnable onLicenseChange;
  private static final Runnable DO_NOTHING_WHEN_LICENSE_CHANGE = () -> {};

  private ActivateStatus oldActivateStatus = ActivateStatus.UNKNOWN;

  // endregion

  public License(Runnable onLicenseChange) {
    this.onLicenseChange = onLicenseChange;
    allLimit =
        Arrays.asList(
            licenseIssueTimestamp,
            licenseExpireTimestamp,
            disconnectionFromActiveNodeTimeLimit,
            skipHardwareSystemInfoCheck,
            dataNodeNumLimit,
            dataNodeCpuCoreNumLimit,
            deviceNumLimit,
            sensorNumLimit,
            aiNodeNumLimit);
  }

  public License() {
    this(DO_NOTHING_WHEN_LICENSE_CHANGE);
  }

  // region getter and setter

  public long getLicenseExpireTimestamp() {
    return licenseExpireTimestamp.getValue();
  }

  public long getLicenseIssueTimestamp() {
    return licenseIssueTimestamp.getValue();
  }

  public short getDataNodeNumLimit() {
    return dataNodeNumLimit.getValue();
  }

  public int getDataNodeCpuCoreNumLimit() {
    return dataNodeCpuCoreNumLimit.getValue();
  }

  public long getDeviceNumLimit() {
    return deviceNumLimit.getValue();
  }

  public long getSensorNumLimit() {
    return sensorNumLimit.getValue();
  }

  public long getDisconnectionFromActiveNodeTimeLimit() {
    return this.disconnectionFromActiveNodeTimeLimit.getValue();
  }

  public short getAINodeNumLimit() {
    return this.aiNodeNumLimit.getValue();
  }

  // endregion

  public boolean reset() {
    if (licenseSource.equals(LicenseSource.NO_LICENSE)) {
      return false;
    }
    for (Limit<?> limit : allLimit) {
      limit.reset();
    }
    licenseSource = LicenseSource.NO_LICENSE;
    this.onLicenseChange.run();
    logActivateStatus(true);
    return true;
  }

  // region load method

  public boolean loadFromProperties(Properties properties, boolean needLog)
      throws LicenseException {
    // try load properties
    License newLicense = new License(null);
    // activate info
    try {
      // To add a new license field, set a default value for compatible with older license version
      newLicense.licenseIssueTimestamp.parse(properties.getProperty(LICENSE_ISSUE_TIMESTAMP_NAME));
      newLicense.licenseExpireTimestamp.parse(
          properties.getProperty(LICENSE_EXPIRE_TIMESTAMP_NAME));
      newLicense.skipHardwareSystemInfoCheck.parse(
          properties.getProperty(SKIP_HARDWARE_SYSTEM_INFO_CHECK_NAME));
      newLicense.dataNodeNumLimit.parse(properties.getProperty(DATANODE_NUM_LIMIT_NAME));
      newLicense.dataNodeCpuCoreNumLimit.parse(
          properties.getProperty(DATANODE_CPU_CORE_NUM_LIMIT_NAME));
      newLicense.deviceNumLimit.parse(properties.getProperty(DEVICE_NUM_LIMIT_NAME));
      newLicense.sensorNumLimit.parse(properties.getProperty(SENSOR_NUM_LIMIT_NAME));
      newLicense.disconnectionFromActiveNodeTimeLimit.parse(
          properties.getProperty(DISCONNECTION_FROM_ACTIVE_NODE_TIME_LIMIT_NAME));
      newLicense.aiNodeNumLimit.parse(properties.getProperty(AINODE_NUM_LIMIT_NAME, "0"));
    } catch (Exception e) {
      logger.error("License parse error", e);
      return false;
    }

    // compare and copy
    if (needLog) {
      this.logLicenseDifferences(newLicense);
    }
    this.copyFrom(newLicense);
    this.licenseSource = LicenseSource.FROM_FILE;

    // if activate status change, log
    if (needLog) {
      logActivateStatus(false);
    }

    this.onLicenseChange.run();

    return true;
  }

  public void loadFromTLicense(TLicense license) throws LicenseException {
    License newLicense = new License(null);
    newLicense.licenseIssueTimestamp.setValue(license.licenseIssueTimestamp);
    newLicense.licenseExpireTimestamp.setValue(license.getExpireTimestamp());
    newLicense.disconnectionFromActiveNodeTimeLimit.setValue(
        license.getDisconnectionFromActiveNodeTime());
    newLicense.dataNodeCpuCoreNumLimit.setValue(license.cpuCoreNum);
    newLicense.dataNodeNumLimit.setValue(license.dataNodeNum);
    newLicense.deviceNumLimit.setValue(license.deviceNum);
    newLicense.sensorNumLimit.setValue(license.sensorNum);
    newLicense.aiNodeNumLimit.setValue(license.getAiNodeNum());

    // compare and copy
    this.logLicenseDifferences(newLicense);
    this.copyFrom(newLicense);
    this.licenseSource = LicenseSource.FROM_REMOTE;

    // if activate status change, log
    logActivateStatus(false);

    this.onLicenseChange.run();
  }

  // endregion

  // region helper method

  private long getLong(String key, Properties properties) throws LicenseException {
    if (!properties.containsKey(key)) {
      throw new LicenseException(
          String.format("%s is necessary, but cannot be found in license file", key));
    }
    try {
      return Long.parseLong(properties.getProperty(key).trim());
    } catch (NumberFormatException e) {
      throw new LicenseException(key + " field cannot be parsed to Long", e);
    }
  }

  public void logActivateStatus(boolean onlyForStatusChange) {
    ActivateStatus nowActivateStatus = getActivateStatus();
    if (onlyForStatusChange) {
      if (!nowActivateStatus.equals(oldActivateStatus)) {
        logger.info(
            "ConfigNode's activation status change: {} -> {}",
            oldActivateStatus,
            nowActivateStatus);
      }
    } else {
      logger.info(
          "ConfigNode's activation status is {}; Previous status is {}",
          nowActivateStatus,
          oldActivateStatus);
    }
    oldActivateStatus = nowActivateStatus;
  }

  private <T> void logFieldDifference(String name, Limit<T> mine, Limit<T> another)
      throws LicenseException {
    if (!Objects.equals(mine.getValue(), another.getValue())) {
      String rawContent = String.format("%s: %s -> %s", name, mine.getValue(), another.getValue());
      String encryptedContent = RSA.publicEncrypt(rawContent);
      logger.info(encryptedContent);
    }
  }

  private void logLicenseDifferences(License anotherLicense) throws LicenseException {
    logFieldDifference(
        "licenseIssueTimestamp", this.licenseIssueTimestamp, anotherLicense.licenseIssueTimestamp);
    logFieldDifference(
        "licenseExpireTimestamp",
        this.licenseExpireTimestamp,
        anotherLicense.licenseExpireTimestamp);
    logFieldDifference(
        "disconnectionFromActiveNodeTimeLimit",
        this.disconnectionFromActiveNodeTimeLimit,
        anotherLicense.disconnectionFromActiveNodeTimeLimit);
    logFieldDifference(
        "skipHardwareSystemInfoCheck",
        this.skipHardwareSystemInfoCheck,
        anotherLicense.skipHardwareSystemInfoCheck);
    logFieldDifference("dataNodeNumLimit", this.dataNodeNumLimit, anotherLicense.dataNodeNumLimit);
    logFieldDifference(
        "dataNodeCpuCoreNumLimit",
        this.dataNodeCpuCoreNumLimit,
        anotherLicense.dataNodeCpuCoreNumLimit);
    logFieldDifference("deviceNumLimit", this.deviceNumLimit, anotherLicense.deviceNumLimit);
    logFieldDifference("sensorNumLimit", this.sensorNumLimit, anotherLicense.sensorNumLimit);
    logFieldDifference("aiNodeNumLimit", this.aiNodeNumLimit, anotherLicense.aiNodeNumLimit);
  }

  // show difference between old license and new license
  private void copyFrom(License anotherLicense) {
    this.licenseIssueTimestamp.setValue(anotherLicense.licenseIssueTimestamp.getValue());
    this.licenseExpireTimestamp.setValue(anotherLicense.licenseExpireTimestamp.getValue());
    this.disconnectionFromActiveNodeTimeLimit.setValue(
        anotherLicense.disconnectionFromActiveNodeTimeLimit.getValue());
    this.skipHardwareSystemInfoCheck.setValue(
        anotherLicense.skipHardwareSystemInfoCheck.getValue());
    this.dataNodeNumLimit.setValue(anotherLicense.dataNodeNumLimit.getValue());
    this.dataNodeCpuCoreNumLimit.setValue(anotherLicense.dataNodeCpuCoreNumLimit.getValue());
    this.deviceNumLimit.setValue(anotherLicense.deviceNumLimit.getValue());
    this.sensorNumLimit.setValue(anotherLicense.sensorNumLimit.getValue());
    this.aiNodeNumLimit.setValue(anotherLicense.aiNodeNumLimit.getValue());
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
        this.getAINodeNumLimit());
  }

  // endregion

  // region show status to outside

  public boolean isActivated() {
    return this.getLicenseExpireTimestamp() >= System.currentTimeMillis();
  }

  public boolean isActive() {
    return LicenseSource.FROM_FILE.equals(licenseSource);
  }

  public boolean noLicense() {
    return LicenseSource.NO_LICENSE.equals(licenseSource);
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

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof License)) {
      return false;
    }
    License another = (License) obj;
    Iterator<Limit<?>> iterator = another.allLimit.iterator();
    try {
      for (Limit<?> limit : allLimit) {
        if (!Objects.equals(limit.getValue(), iterator.next().getValue())) {
          return false;
        }
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }
}
