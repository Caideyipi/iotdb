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
package com.timecho.iotdb.service;

import org.apache.iotdb.common.rpc.thrift.TLicense;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.rpc.thrift.TShowActivationResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.LicenseInfoResp;

import org.apache.thrift.TException;

public class ClientRPCServiceImplNew extends ClientRPCServiceImpl {

  public static final String ROOT_USER = "root";

  @Override
  public LicenseInfoResp getLicenseInfo() throws TException {
    try (ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TShowActivationResp resp = configNodeClient.showActivation();
      TLicense license = resp.getLicense();
      TLicense usage = resp.getUsage();
      return new LicenseInfoResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
          .setIsActive(!CommonDescriptor.getInstance().getConfig().isUnactivated())
          .setExpireDate(
              DateTimeUtils.convertMillsecondToZonedDateTime(license.expireTimestamp).toString())
          .setIsEnterprise(true)
          .setLicense(license)
          .setUsage(usage);
    } catch (ClientManagerException e) {
      throw new TException(e);
    }
  }
}
