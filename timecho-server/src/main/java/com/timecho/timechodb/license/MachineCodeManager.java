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

import cn.hutool.system.OsInfo;
import cn.hutool.system.SystemUtil;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MachineCodeManager extends LicenseManager {
  private static final Logger logger = LoggerFactory.getLogger(MachineCodeManager.class);
  private SystemInfo systemInfo;

  public void init() {
    SystemInfoService systemInfoService;
    OsInfo osInfo = SystemUtil.getOsInfo();
    if (osInfo.isWindows()) {
      systemInfoService = new WindowsSystem();
    } else if (osInfo.isMac()) {
      systemInfoService = new MacOsSystem();
    } else {
      systemInfoService = new LinuxSystem();
    }
    try {
      this.systemInfo = new SystemInfo();
      this.systemInfo.setCpu(systemInfoService.getCPUSerial());
      this.systemInfo.setMainBoard(systemInfoService.getMainBoardSerial());
    } catch (Exception e) {
      logger.error("init error,{}", e.getMessage(), e);
    }
  }

  public String getMachineCodeCipher() throws LicenseException {
    try {
      return Rsa.publicEncrypt(Base64.encodeBase64String(getMachineCode().getBytes()));

    } catch (Exception e) {
      throw new LicenseException("get machine code error");
    }
  }

  public String getMachineCode() {
    return this.systemInfo.toString();
  }

  public static MachineCodeManager getInstance() {
    return MachineCodeManager.InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static MachineCodeManager instance = new MachineCodeManager();
  }
}
