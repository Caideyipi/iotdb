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
import org.apache.iotdb.db.service.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.LicenseInfoResp;
import org.apache.iotdb.service.rpc.thrift.WhiteListInfoResp;

import com.timecho.timechodb.license.LicenseManager;
import com.timecho.timechodb.rpc.IPFilter;
import org.apache.thrift.TException;

import java.util.Set;

public class ClientRPCServiceImplNew extends ClientRPCServiceImpl {

  @Override
  public WhiteListInfoResp getWhiteIpSet() throws TException {
    WhiteListInfoResp whiteListInfoResp =
        new WhiteListInfoResp(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    whiteListInfoResp.setWhiteList(IPFilter.getInstance().getAllowListPatterns());
    return whiteListInfoResp;
  }

  @Override
  public TSStatus updateWhiteList(Set<String> ipSet) throws TException {
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
