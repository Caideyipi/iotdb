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

import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.manager.TimechoConfigManagerForOtherIT;

import com.timecho.iotdb.service.ConfigNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

public class ConfigNodeForOtherIT extends ConfigNode {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeForOtherIT.class);

  public static void main(String[] args) {
    LOGGER.info(
        "{} environment variables: {}",
        ConfigNodeConstant.GLOBAL_NAME,
        ConfigNodeConfig.getEnvironmentVariables());
    LOGGER.info(
        "{} default charset is: {}",
        ConfigNodeConstant.GLOBAL_NAME,
        Charset.defaultCharset().displayName());
    ConfigNodeForOtherIT configNode = new ConfigNodeForOtherIT();
    int returnCode = configNode.run(args);
    if (returnCode != 0) {
      System.exit(returnCode);
    }
    org.apache.iotdb.confignode.service.ConfigNode.setInstance(configNode);
  }

  @Override
  public void setConfigManager() throws Exception {
    timechoConfigManager = new TimechoConfigManagerForOtherIT();
    super.configManager = timechoConfigManager;
  }
}
