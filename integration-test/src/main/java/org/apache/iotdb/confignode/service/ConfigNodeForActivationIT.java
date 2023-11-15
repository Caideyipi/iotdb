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

package org.apache.iotdb.confignode.service;

import org.apache.iotdb.commons.exception.LicenseException;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.manager.ConfigManagerForActivationIT;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;

public class ConfigNodeForActivationIT extends ConfigNode {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeForActivationIT.class);

  public static void main(String[] args) {
    LOGGER.info(
        "{} environment variables: {}",
        ConfigNodeConstant.GLOBAL_NAME,
        ConfigNodeConfig.getEnvironmentVariables());
    LOGGER.info(
        "{} default charset is: {}",
        ConfigNodeConstant.GLOBAL_NAME,
        Charset.defaultCharset().displayName());
    new ConfigNodeCommandLineForActivationIT().doMain(args);
  }

  @Override
  void initConfigManager() {
    try {
      configManager = new ConfigManagerForActivationIT();
    } catch (IOException e) {
      LOGGER.error("Can't start ConfigNode consensus group!", e);
      stop();
    }
    try {
      configManager.initActivationManager();
    } catch (LicenseException e) {
      LOGGER.error("init license manager fail!");
      stop();
    }
    // Add some Metrics for configManager
    configManager.addMetrics();
    LOGGER.info("Successfully initialize ConfigManager.");
  }

  @Override
  protected void addShutDownHook() {
    Runtime.getRuntime().addShutdownHook(new ConfigNodeShutdownHookForActivationIT());
  }

  private static class ConfigNodeHolder {

    private static final ConfigNodeForActivationIT INSTANCE = new ConfigNodeForActivationIT();

    private ConfigNodeHolder() {
      // Empty constructor
    }
  }

  public static ConfigNodeForActivationIT getInstance() {
    return ConfigNodeHolder.INSTANCE;
  }
}
