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

import cn.hutool.system.oshi.OshiUtil;
import oshi.hardware.ComputerSystem;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MacOsSystem extends SystemInfoService {

  @Override
  protected List<String> getMacAddress() throws LicenseException {
    List<String> result = null;
    List<InetAddress> inetAddresses = getLocalAllInetAddress();
    if (!inetAddresses.isEmpty()) {
      List<String> list = new ArrayList<>();
      Set<String> uniqueValues = new HashSet<>();
      for (InetAddress inetAddress : inetAddresses) {
        String macByInetAddress = getMacByInetAddress(inetAddress);
        if (uniqueValues.add(macByInetAddress)) {
          list.add(macByInetAddress);
        }
      }
      result = list;
    }
    return result;
  }

  @Override
  protected String getCPUSerial() throws LicenseException {

    ComputerSystem system = OshiUtil.getSystem();
    return system.getHardwareUUID();
  }

  @Override
  protected String getMainBoardSerial() throws LicenseException {
    ComputerSystem system = OshiUtil.getSystem();
    return system.getBaseboard().getSerialNumber();
  }
}
