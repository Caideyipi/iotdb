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

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class SystemInfoService {
  protected List<String> getMacAddress() throws LicenseException {
    List<String> result = new ArrayList<>();
    List<InetAddress> inetAddresses = getLocalAllInetAddress();
    if (!inetAddresses.isEmpty()) {
      Set<String> uniqueValues = new HashSet<>();
      for (InetAddress inetAddress : inetAddresses) {
        String macByInetAddress = getMacByInetAddress(inetAddress);
        if (uniqueValues.add(macByInetAddress)) {
          result.add(macByInetAddress);
        }
      }
    }
    return result;
  }

  protected abstract String getCPUSerial() throws LicenseException;

  protected abstract String getMainBoardSerial() throws LicenseException;

  protected List<InetAddress> getLocalAllInetAddress() throws LicenseException {
    List<InetAddress> result = new ArrayList<>(4);
    try {
      for (Enumeration<NetworkInterface> networkInterfaces =
              NetworkInterface.getNetworkInterfaces();
          networkInterfaces.hasMoreElements(); ) {
        NetworkInterface iface = networkInterfaces.nextElement();
        for (Enumeration<InetAddress> inetAddresses = iface.getInetAddresses();
            inetAddresses.hasMoreElements(); ) {
          InetAddress inetAddr = inetAddresses.nextElement();
          if (!inetAddr.isLoopbackAddress()
              && !inetAddr.isLinkLocalAddress()
              && !inetAddr.isMulticastAddress()) {
            result.add(inetAddr);
          }
        }
      }
    } catch (SocketException e) {
      throw new LicenseException("Internet address information cannot be read");
    }

    return result;
  }

  protected String getMacByInetAddress(InetAddress inetAddr) throws LicenseException {
    try {
      byte[] mac = NetworkInterface.getByInetAddress(inetAddr).getHardwareAddress();
      StringBuilder stringBuffer = new StringBuilder();

      for (int i = 0; i < mac.length; i++) {
        if (i != 0) {
          stringBuffer.append("-");
        }

        String temp = Integer.toHexString(mac[i] & 0xff);
        if (temp.length() == 1) {
          stringBuffer.append("0").append(temp);
        } else {
          stringBuffer.append(temp);
        }
      }
      return stringBuffer.toString().toUpperCase();
    } catch (SocketException e) {
      throw new LicenseException("Internet address information cannot be read");
    }
  }
}
