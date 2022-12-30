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

import java.io.IOException;
import java.util.Scanner;

public class WindowsSystem extends SystemInfoService {

  @Override
  protected String getCPUSerial() throws LicenseException {
    try {
      return executeShell("wmic cpu get processorid");
    } catch (Exception e) {
      throw new LicenseException("CPU information cannot be read");
    }
  }

  @Override
  protected String getMainBoardSerial() throws LicenseException {
    try {
      return executeShell("wmic baseboard get serialnumber");
    } catch (Exception e) {
      throw new LicenseException("MainBoard information cannot be read");
    }
  }

  protected static String executeShell(String shell) throws IOException {
    Process process = Runtime.getRuntime().exec(shell);
    process.getOutputStream().close();
    Scanner scanner = new Scanner(process.getInputStream());
    if (scanner.hasNext()) {
      scanner.next();
    }
    String serialNumber = "";
    if (scanner.hasNext()) {
      serialNumber = scanner.next().trim();
    }

    scanner.close();
    return serialNumber;
  }
}
