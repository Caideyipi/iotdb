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

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class LinuxSystem extends SystemInfoService {

  @Override
  protected String getCPUSerial() throws LicenseException {
    try {
      String serialNumber = "";

      String[] shell = {
        "/bin/bash",
        "-c",
        "dmidecode -t processor | grep 'ID' | awk -F ':' '{print $2}' | head -n 1"
      };
      return executeShell(serialNumber, shell);
    } catch (Exception e) {
      throw new LicenseException("CPU information cannot be read");
    }
  }

  @Override
  protected String getMainBoardSerial() throws LicenseException {
    try {
      String serialNumber = "";
      String[] shell = {
        "/bin/bash", "-c", "dmidecode | grep 'Serial Number' | awk -F ':' '{print $2}' | head -n 1"
      };
      return executeShell(serialNumber, shell);
    } catch (Exception e) {
      throw new LicenseException("MainBoard information cannot be read");
    }
  }

  protected String executeShell(String serialNumber, String[] shell) throws Exception {
    Process process = Runtime.getRuntime().exec(shell);
    process.getOutputStream().close();
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String line = reader.readLine().trim();
    if (!StringUtils.isEmpty(line)) {
      serialNumber = line;
    }
    reader.close();
    return serialNumber;
  }
}
