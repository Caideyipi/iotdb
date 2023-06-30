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
package com.timecho.iotdb.license;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;

import com.timecho.iotdb.utils.DateUtil;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.timecho.iotdb.utils.DateUtil.FORMAT;

public class LicenseCheckService implements Runnable, IService {
  private static final Logger logger = LoggerFactory.getLogger(LicenseCheckService.class);
  private ScheduledExecutorService executor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("License-Check");

  @Override
  public void start() throws StartupException {
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(executor, this, 0, 1, TimeUnit.DAYS);
    logger.info("license check service start successfully");
  }

  public void stop() {
    executor.shutdownNow();
  }

  @Override
  public void waitAndStop(long milliseconds) {
    executor.shutdownNow();
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {
    executor.shutdownNow();
  }

  @Override
  public ServiceType getID() {
    return null;
  }

  @Override
  public void run() {
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    LicenseManager licenseManager = LicenseManager.getInstance();
    try {
      String licenseStr = licenseManager.read(LicenseManager.LICENSE_FILE_PATH);
      licenseManager.loadLicense(licenseStr);

      String expireDate = licenseManager.getExpireDate();
      String startDate = licenseManager.getStartDate();
      String today = DateUtil.format(new Date(), FORMAT);
      if ((today.compareTo(startDate) < 0 || today.compareTo(expireDate) > 0)
          && config.getNodeStatus() == NodeStatus.Running) {
        logger.error(
            "The license has expired,system has been set to READ_ONLY,contact the service provider for reauthorization");
        config.setNodeStatus(NodeStatus.ReadOnly);
      }
      int dateSub = DateUtil.timeSub(today, expireDate);
      if (dateSub < 15) {
        logger.error("License expires in {} days,please reauthorize as soon as possible", dateSub);
      }

      // write time to file
      licenseManager.write(
          Base64.encodeBase64String(licenseManager.getLastDate().getBytes()),
          LicenseManager.EXPIRE_FILE_PATH);
    } catch (LicenseException e) {
      config.setNodeStatus(NodeStatus.ReadOnly);
      logger.error("license file not found,system status set to READ_ONLY");
    }
  }

  public static LicenseCheckService getInstance() {
    return LicenseCheckServiceHolder.INSTANCE;
  }

  private static class LicenseCheckServiceHolder {
    private static final LicenseCheckService INSTANCE = new LicenseCheckService();

    private LicenseCheckServiceHolder() {}
  }
}
