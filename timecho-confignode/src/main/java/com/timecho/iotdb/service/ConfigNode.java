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

import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCServiceProcessor;

import com.timecho.iotdb.commons.secret.SecretKey;
import com.timecho.iotdb.commons.utils.OSUtils;
import com.timecho.iotdb.manager.TimechoConfigManager;
import com.timecho.iotdb.manager.activation.ActivationManager;
import com.timecho.iotdb.service.thrift.TimechoConfigNodeRPCServiceProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

public class ConfigNode extends org.apache.iotdb.confignode.service.ConfigNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNode.class);

  protected TimechoConfigManager timechoConfigManager;

  public static void main(String[] args) {
    LOGGER.info(
        "{} environment variables: {}",
        ConfigNodeConstant.GLOBAL_NAME,
        ConfigNodeConfig.getEnvironmentVariables());
    LOGGER.info(
        "{} default charset is: {}",
        ConfigNodeConstant.GLOBAL_NAME,
        Charset.defaultCharset().displayName());
    ConfigNode configNode = new ConfigNode();
    int returnCode = configNode.run(args);
    if (returnCode != 0) {
      System.exit(returnCode);
    }
    org.apache.iotdb.confignode.service.ConfigNode.setInstance(configNode);
  }

  @Override
  protected void generateSystemInfoFile() {
    ActivationManager.generateSystemInfoFile();
  }

  @Override
  protected void setConfigManager() throws Exception {
    this.timechoConfigManager = new TimechoConfigManager();
    super.configManager = this.timechoConfigManager;
  }

  @Override
  protected ConfigNodeRPCServiceProcessor getConfigNodeRPCServiceProcessor() {
    return new TimechoConfigNodeRPCServiceProcessor(timechoConfigManager);
  }

  @Override
  protected void saveSecretKey() {
    SecretKey.getInstance()
        .initSecretKeyFile(
            ConfigNodeDescriptor.getInstance().getConf().getSystemDir(),
            String.valueOf(UUID.randomUUID()));
  }

  @Override
  protected void saveHardwareCode() {
    try {
      String hardwareCode = OSUtils.generateSystemInfoContentWithVersion();
      SecretKey.getInstance()
          .initHardwareCodeFile(
              ConfigNodeDescriptor.getInstance().getConf().getSystemDir(), hardwareCode);
    } catch (Exception e) {
      LOGGER.error("hardware generation failed.");
    }
  }

  @Override
  protected void loadSecretKey() throws IOException {
    SecretKey.getInstance()
        .loadSecretKeyFromFile(ConfigNodeDescriptor.getInstance().getConf().getSystemDir());
  }

  @Override
  protected void loadHardwareCode() throws IOException {
    SecretKey.getInstance()
        .loadHardwareCodeFromFile(ConfigNodeDescriptor.getInstance().getConf().getSystemDir());
  }
}
