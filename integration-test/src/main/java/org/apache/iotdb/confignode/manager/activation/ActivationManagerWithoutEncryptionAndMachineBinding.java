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

package org.apache.iotdb.confignode.manager.activation;

import org.apache.iotdb.commons.exception.LicenseException;
import org.apache.iotdb.confignode.manager.ConfigManager;

import java.util.Properties;

/** License Manager without system info check, only for IoTDBActivationTest */
public class ActivationManagerWithoutEncryptionAndMachineBinding extends ActivationManager {

  public ActivationManagerWithoutEncryptionAndMachineBinding(ConfigManager configManager)
      throws LicenseException {
    super(configManager);
  }

  @Override
  public boolean verifyAllSystemInfo(Properties properties) {
    return true;
  }

  @Override
  protected String encrypt(String src) {
    return src;
  }

  @Override
  protected String decrypt(String src) {
    return src;
  }
}
