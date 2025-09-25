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

package org.apache.iotdb.confignode.manager.regulate;

import org.apache.iotdb.common.rpc.thrift.TLicense;
import org.apache.iotdb.commons.exception.LicenseException;

import com.timecho.iotdb.manager.TimechoConfigManager;
import com.timecho.iotdb.manager.regulate.RegulateManager;

import java.util.Properties;

/** Only for integration test (except IoTDBActivationTest) */
public class RegulateManagerWithoutAnyLimit extends RegulateManager {

  public RegulateManagerWithoutAnyLimit(TimechoConfigManager configManager)
      throws LicenseException {
    super(configManager);
    this.lottery =
        new LotteryWithoutLimit(
            () ->
                configManager
                    .getClusterSchemaManager()
                    .updateSchemaQuotaConfiguration(
                        LotteryWithoutLimit.DEVICE_NUM_UNLIMITED,
                        LotteryWithoutLimit.SENSOR_NUM_UNLIMITED));
  }

  @Override
  public boolean verifyAllSystemInfo(Properties licenseProperties) {
    return true;
  }

  /** Override this method in order to make sure license won't be reloaded. */
  @Override
  public void tryLoadRemoteLicense(TLicense remoteLicense) {
    // do nothing
  }

  /** Override this method in order to make sure license won't be reloaded. */
  @Override
  protected void tryLoadLicenseFromFile() {
    // do nothing
  }
}
