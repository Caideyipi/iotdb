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

package com.timecho.iotdb.manager.regulate;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;

import com.google.common.collect.ImmutableMap;
import com.timecho.iotdb.commons.commission.Bandit;
import com.timecho.iotdb.commons.commission.Lottery;
import com.timecho.iotdb.commons.systeminfo.SystemInfoGetter;
import com.timecho.iotdb.commons.utils.OSUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

import static java.lang.System.exit;

public class RegulateVerifier {
  private static final int SUCCESS_CODE = 0;
  private static final int FAIL_CODE = 1;
  private static final int UNABLE_TO_VERIFY_CODE = 2;
  private static final String LOAD_FAIL_MESSAGE =
      "License verification failed. Please check your license.";
  private static final String FAIL_MESSAGE =
      "License verification failed. Please contact Timecho for more information.";
  private static final String UNABLE_TO_VERIFY_MESSAGE =
      "Cannot verify the license. You may start the cluster and use 'show cluster' to check the activation status.";

  public static void main(String[] args) {
    Properties licenseProperties = loadLicense(args);
    Properties systemProperties = loadSystemProperties();
    checkSystemInfo(systemProperties, licenseProperties);
    checkLicense(licenseProperties);
    System.out.println(
        "License has been successfully verified. Now, it is recommended to start the cluster, then use 'show cluster' and 'show activation' to perform further verification of the activation status.");
    exit(SUCCESS_CODE);
  }

  private static Properties loadLicense(String[] args) {
    if (args.length != 1) {
      String str = Arrays.toString(args);
      errorThenExit("Unexpected args %s, expect [%%licenseFilePath%%]", str);
    }
    String licenseFilePath = args[0];
    Properties licenseProperties = new Properties();

    String encryptedLicenseContent;
    try (BufferedReader reader = new BufferedReader(new FileReader(licenseFilePath))) {
      StringBuilder stringBuilder = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        stringBuilder.append(line);
      }
      encryptedLicenseContent = stringBuilder.toString();
      licenseProperties =
          RegulateManager.loadLicenseFromEveryVersionStatic(encryptedLicenseContent);
    } catch (Exception e) {
      System.out.println(exceptionToEncryptedString(e));
      errorThenExit(LOAD_FAIL_MESSAGE);
    }
    return licenseProperties;
  }

  private static Properties loadSystemProperties() {
    String homePath = System.getProperty(ConfigNodeConstant.CONFIGNODE_HOME);
    String confDir = homePath + File.separator + IoTDBConstant.CN_DEFAULT_DATA_DIR;
    String confVar = System.getProperty(ConfigNodeConstant.CONFIGNODE_CONF);
    if (confVar != null && !confVar.isEmpty()) {
      confDir = confVar;
    }
    String confPropertiesPath = confDir + File.separator + CommonConfig.SYSTEM_CONFIG_NAME;
    Properties confProperties;
    try {
      confProperties = pathToProperties(confPropertiesPath);
    } catch (IOException e) {
      confProperties = new Properties();
    }

    String systemDir =
        Optional.ofNullable(confProperties.get("cn_system_dir"))
            .map(String::valueOf)
            .orElse(
                homePath
                    + File.separator
                    + IoTDBConstant.CN_DEFAULT_DATA_DIR
                    + File.separator
                    + IoTDBConstant.SYSTEM_FOLDER_NAME);
    String systemPropertiesPath = systemDir + File.separator + ConfigNodeConstant.SYSTEM_FILE_NAME;
    System.out.println(systemPropertiesPath);
    Properties systemProperties = null;
    try {
      systemProperties = pathToProperties(systemPropertiesPath);
    } catch (IOException e) {
      // Cannot find system.properties
      unableToVerifyThenExit(UNABLE_TO_VERIFY_MESSAGE);
    }

    return systemProperties;
  }

  private static void checkSystemInfo(Properties systemProperties, Properties licenseProperties) {
    SystemInfoGetter systemInfoGetter = OSUtils.generateSystemInfoGetter();
    systemInfoGetter.setLogEnabled(false);

    ImmutableMap<String, Supplier<String>> configurableSystemInfoNameToItsGetter =
        ImmutableMap.of(
            Lottery.IP_ADDRESS_NAME,
            () -> String.valueOf(systemProperties.get(IoTDBConstant.CN_INTERNAL_ADDRESS)),
            Lottery.INTERNAL_PORT_NAME,
            () -> String.valueOf(systemProperties.get(IoTDBConstant.CN_INTERNAL_PORT)),
            Lottery.IS_SEED_CONFIGNODE_NODE_NAME,
            () -> String.valueOf(systemProperties.get("is_seed_config_node")));

    if (!RegulateManager.verifyAllSystemInfoOfEveryVersion(
        licenseProperties,
        OSUtils.hardwareSystemInfoNameToItsGetter,
        configurableSystemInfoNameToItsGetter)) {
      errorThenExit(FAIL_MESSAGE);
    }
  }

  private static void checkLicense(Properties licenseProperties) {
    Lottery lottery = new Lottery(() -> {});
    try {
      lottery.loadFromProperties(licenseProperties, false);
    } catch (Exception e) {
      System.out.println(exceptionToEncryptedString(e));
      errorThenExit(FAIL_MESSAGE);
    }
    if (!RegulateManager.checkSystemTimeAndIssueTimeImpl(lottery)) {
      System.out.println("Time check failed");
      errorThenExit(FAIL_MESSAGE);
    }
  }

  private static Properties pathToProperties(String filePath) throws IOException {
    Properties properties = new Properties();
    try (FileReader reader = new FileReader(filePath)) {
      properties.load(reader);
    }
    return properties;
  }

  private static void errorThenExit(String pattern, Object... args) {
    System.out.printf(pattern + "\n", args);
    exit(FAIL_CODE);
  }

  private static void unableToVerifyThenExit(String pattern, Object... args) {
    System.out.printf(pattern + "\n", args);
    exit(UNABLE_TO_VERIFY_CODE);
  }

  private static String exceptionToEncryptedString(Exception e) {
    StringWriter writer = new StringWriter();
    e.printStackTrace(new PrintWriter(writer));
    try {
      return Bandit.publicEncrypt(writer.toString());
    } catch (Exception ee) {
      return e.getMessage();
    }
  }
}
