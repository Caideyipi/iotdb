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
package com.timecho.timechodb;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.service.DataNodeInternalRPCService;
import org.apache.iotdb.db.service.RPCService;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import com.timecho.timechodb.conf.TimechoDBDescriptor;
import com.timecho.timechodb.license.LicenseCheckService;
import com.timecho.timechodb.license.LicenseException;
import com.timecho.timechodb.license.LicenseManager;
import com.timecho.timechodb.license.MachineCodeManager;
import com.timecho.timechodb.service.ClientRPCServiceImplNew;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;
import java.util.Scanner;

import static com.timecho.timechodb.license.LicenseManager.LICENSE_FILE_PATH;
import static com.timecho.timechodb.license.LicenseManager.LICENSE_PATH;

public class DataNode extends org.apache.iotdb.db.service.DataNode {
  private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

  private static final String CONFIG_FILE = "timechodb-datanode.properties";

  public static void main(String[] args) {
    try {
      // init licenseManager and machineCodeManager
      TimechoDBDescriptor.getInstance().loadTimechoDBProperties(CONFIG_FILE);
      LicenseManager licenseManager = LicenseManager.getInstance();
      MachineCodeManager machineCodeManager = MachineCodeManager.getInstance();
      machineCodeManager.init();
      String systemInfo = machineCodeManager.getMachineCodeCipher();
      File licenseFile = new File(LICENSE_FILE_PATH);
      String licenseStr;
      if (!licenseFile.exists()) {
        logger.info(
            "\t\n=============Copy the machine code and contact the service provider==============\t\n{}\t\n=================================================================================\t\nTimechoDB is not activated. Please activate it:",
            systemInfo);
        Scanner scanner = new Scanner(System.in);
        licenseStr = scanner.next();

        File licenseDirectoryFile = new File(LICENSE_PATH);
        // If this is the first startup, license file is not exists,need to activate.else read from
        // license file.
        if (!licenseDirectoryFile.exists() && !licenseDirectoryFile.mkdirs()) {
          logger.info("create directory error, insufficient permissions.");
          System.exit(-1);
        }
        licenseManager.loadLicense(licenseStr);
        if (!licenseManager.verify(machineCodeManager.getMachineCode())) {
          logger.info("license verify error,please check license");
          System.exit(-1);
        }
        licenseManager.write(licenseStr, LICENSE_FILE_PATH);
      } else {
        licenseStr = licenseManager.read(LICENSE_FILE_PATH);
        licenseManager.loadLicense(licenseStr);
      }
      // check license effective
      if (licenseManager.verify(machineCodeManager.getMachineCode())) {
        registerManager.register(LicenseCheckService.getInstance());
        // rewrite
        Properties properties = TimechoDBDescriptor.getInstance().getCustomizedProperties();
        CommonDescriptor.getInstance().loadCommonProps(properties);
        IoTDBDescriptor.getInstance().loadProperties(properties);
        IoTDBDescriptor.getInstance().getConfig().setCustomizedProperties(properties);
        TSFileDescriptor.getInstance().overwriteConfigByCustomSettings(properties);
        TSFileDescriptor.getInstance().getConfig().setCustomizedProperties(properties);
        SchemaEngine.getInstance().getSeriesNumerMonitor().init(null);
        new DataNodeServerCommandLineNew().doMain(args);
        logger.info("The license expires at {}", LicenseManager.getInstance().getExpireDate());
      } else {
        logger.info("license verify error,please check license");
      }

    } catch (StartupException e) {
      logger.error("system init error,", e);
      System.exit(-1);
    } catch (LicenseException e) {
      logger.error("license error,", e);
      System.exit(-1);
    }
  }

  @Override
  protected void setUpRPCService() throws StartupException {
    // Start InternalRPCService to indicate that the current DataNode can accept cluster scheduling
    registerManager.register(DataNodeInternalRPCService.getInstance());

    // Notice: During the period between starting the internal RPC service
    // and starting the client RPC service , some requests may fail because
    // DataNode is not marked as RUNNING by ConfigNode-leader yet.

    // Start client RPCService to indicate that the current DataNode provide external services
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setRpcImplClassName(ClientRPCServiceImplNew.class.getName());
    if (config.isEnableRpcService()) {
      registerManager.register(RPCService.getInstance());
    }
    // init service protocols
    initProtocols();
  }

  private static class DataNodeNewHolder {

    private static final DataNode INSTANCE = new DataNode();

    private DataNodeNewHolder() {}
  }

  public static DataNode getInstance() {
    return DataNode.DataNodeNewHolder.INSTANCE;
  }
}
