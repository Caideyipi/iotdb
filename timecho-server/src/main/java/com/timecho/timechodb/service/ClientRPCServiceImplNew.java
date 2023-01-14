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
package com.timecho.timechodb.service;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.service.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.LicenseInfoResp;
import org.apache.iotdb.service.rpc.thrift.WhiteListInfoResp;

import com.timecho.timechodb.license.LicenseManager;
import com.timecho.timechodb.rpc.IPFilter;
import org.apache.thrift.TException;

import java.util.Set;
import java.util.regex.Pattern;

public class ClientRPCServiceImplNew extends ClientRPCServiceImpl {
  public static final String WHITE_LIST_PATTERN =
      "(\\*|25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)\\.(\\*|25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)\\.(\\*|25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)\\.(\\*|25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)";

  public static final String ROOT_USER = "root";

  @Override
  public WhiteListInfoResp getWhiteIpSet() throws TException {
    WhiteListInfoResp whiteListInfoResp =
        new WhiteListInfoResp(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    whiteListInfoResp.setWhiteList(IPFilter.getInstance().getAllowListPatterns());
    return whiteListInfoResp;
  }

  @Override
  public TSStatus updateWhiteList(Set<String> ipSet) throws TException {
    if (ipSet == null) {
      TSStatus status = new TSStatus(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode());
      status.setMessage("illegal parameter");
      return status;
    }
    boolean illegal = ipSet.stream().anyMatch(ip -> !Pattern.matches(WHITE_LIST_PATTERN, ip));
    if (illegal) {
      TSStatus status = new TSStatus(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode());
      status.setMessage("illegal parameter");
      return status;
    }
    if (!ROOT_USER.equals(SessionManager.getInstance().getCurrSession().getUsername())) {
      TSStatus status = new TSStatus(TSStatusCode.NO_PERMISSION_ERROR.getStatusCode());
      status.setMessage(
          "current user have no permission,only the root user can perform this operation");
      return status;
    }
    IPFilter.getInstance().setAllowListPatterns(ipSet);
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public LicenseInfoResp getLicenseInfo() throws TException {
    LicenseInfoResp licenseInfoResp =
        new LicenseInfoResp(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    licenseInfoResp.setExpireDate(LicenseManager.getInstance().getExpireDate());
    licenseInfoResp.setIsActive(true);
    licenseInfoResp.setIsEnterprise(true);
    return licenseInfoResp;
  }
}
