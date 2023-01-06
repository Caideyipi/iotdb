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
package com.timecho.timechodb.license;

import org.apache.iotdb.commons.conf.IoTDBConstant;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson2.JSON;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

public class LicenseManager implements LicenseVerifier {
  public static final String LICENSE_PATH =
      System.getProperty(IoTDBConstant.IOTDB_HOME) + "/data/system/license";
  public static final String LICENSE_FILE_PATH =
      System.getProperty(IoTDBConstant.IOTDB_HOME) + "/data/system/license/active.license";
  public static final String EXPIRE_FILE_PATH =
      System.getProperty(IoTDBConstant.IOTDB_HOME) + "/data/system/license/expire.license";
  private LicenseContent licenseContent;

  private String lastDate = DateUtil.format(new Date(), "yyyy-MM-dd");

  public LicenseContent getLicense() {
    return this.licenseContent;
  }

  public void loadLicense(String licenseContent) throws LicenseException {
    try {
      this.licenseContent =
          JSON.parseObject(Rsa.publicDecrypt(licenseContent), LicenseContent.class);
    } catch (Exception e) {
      throw new LicenseException("load license error,illegal license");
    }
  }

  public void write(String licenseStr, String path) throws LicenseException {
    try (FileWriter fileWriter = new FileWriter(path)) {
      fileWriter.write(licenseStr);
    } catch (Exception e) {
      throw new LicenseException("write license error,insufficient permissions");
    }
  }

  public String read(String path) throws LicenseException {
    try (BufferedReader br = new BufferedReader(new FileReader(path))) {
      return br.readLine();
    } catch (IOException e) {
      throw new LicenseException("file not found");
    }
  }

  public long getMaxAllowedTimeSeriesNumber() {
    return this.licenseContent.getMaxAllowedTimeSeriesNumber();
  }

  public long getMaxInputFrequence() {
    return this.licenseContent.getMaxInputFrequence();
  }

  public String getExpireDate() {
    return this.licenseContent.getExpireDate();
  }

  public static LicenseManager getInstance() {
    return LicenseManager.InstanceHolder.instance;
  }

  public String getLastDate() {
    return lastDate;
  }

  public void setLastDate(String lastDate) {
    this.lastDate = lastDate;
  }

  @Override
  public boolean verify(SystemInfo var1) {
    if (licenseContent == null) {
      return false;
    }
    return licenseContent.getCpu().equals(var1.getCpu())
        && licenseContent.getMainBoard().equals(var1.getMainBoard())
        && licenseContent.getExpireDate().compareTo(DateUtil.format(new Date(), "yyyy-MM-dd")) > -1;
  }

  @Override
  public boolean verify(String var1) {
    SystemInfo systemInfo = JSON.parseObject(var1, SystemInfo.class);
    return verify(systemInfo);
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static LicenseManager instance = new LicenseManager();
  }
}
