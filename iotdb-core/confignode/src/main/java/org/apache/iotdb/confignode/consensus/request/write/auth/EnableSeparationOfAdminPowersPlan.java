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

package org.apache.iotdb.confignode.consensus.request.write.auth;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class EnableSeparationOfAdminPowersPlan extends ConfigPhysicalPlan {

  private String systemAdminUsername;
  private String securityAdminUsername;
  private String auditAdminUsername;

  public EnableSeparationOfAdminPowersPlan() {
    super(ConfigPhysicalPlanType.EnableSeparationOfAdminPowers);
  }

  public EnableSeparationOfAdminPowersPlan(
      String systemAdminUsername, String securityAdminUsername, String auditAdminUsername) {
    super(ConfigPhysicalPlanType.EnableSeparationOfAdminPowers);
    this.systemAdminUsername = systemAdminUsername;
    this.securityAdminUsername = securityAdminUsername;
    this.auditAdminUsername = auditAdminUsername;
  }

  public String getSystemAdminUsername() {
    return systemAdminUsername;
  }

  public void setSystemAdminUsername(String systemAdminUsername) {
    this.systemAdminUsername = systemAdminUsername;
  }

  public String getSecurityAdminUsername() {
    return securityAdminUsername;
  }

  public void setSecurityAdminUsername(String securityAdminUsername) {
    this.securityAdminUsername = securityAdminUsername;
  }

  public String getAuditAdminUsername() {
    return auditAdminUsername;
  }

  public void setAuditAdminUsername(String auditAdminUsername) {
    this.auditAdminUsername = auditAdminUsername;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().getPlanType(), stream);
    ReadWriteIOUtils.write(systemAdminUsername, stream);
    ReadWriteIOUtils.write(securityAdminUsername, stream);
    ReadWriteIOUtils.write(auditAdminUsername, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.systemAdminUsername = ReadWriteIOUtils.readString(buffer);
    this.securityAdminUsername = ReadWriteIOUtils.readString(buffer);
    this.auditAdminUsername = ReadWriteIOUtils.readString(buffer);
  }
}
