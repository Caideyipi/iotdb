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
package com.timecho.iotdb;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;

import com.timecho.iotdb.commons.secret.SecretKey;
import com.timecho.iotdb.commons.utils.OSUtils;
import com.timecho.iotdb.dataregion.migration.MigrationTaskManager;
import com.timecho.iotdb.rpc.IPFilter;
import com.timecho.iotdb.schemaregion.EnterpriseSchemaConstant;
import com.timecho.iotdb.schemaregion.mtree.EnterpriseCachedMNodeFactory;
import com.timecho.iotdb.schemaregion.mtree.EnterpriseMemMNodeFactory;
import com.timecho.iotdb.service.ClientRPCServiceImplNew;
import com.timecho.iotdb.service.DataNodeInternalRPCServiceNew;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.function.Supplier;

public class DataNode extends org.apache.iotdb.db.service.DataNode {
  private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

  public static void main(String[] args) {
    startUp(args, DataNode::new);
  }

  protected static void startUp(String[] args, Supplier<DataNode> dataNodeSupplier) {
    // set up environment for schema region
    MNodeFactoryLoader.getInstance().addNodeFactory(EnterpriseMemMNodeFactory.class);
    MNodeFactoryLoader.getInstance().addNodeFactory(EnterpriseCachedMNodeFactory.class);
    MNodeFactoryLoader.getInstance().setEnv(EnterpriseSchemaConstant.ENTERPRISE_MNODE_FACTORY_ENV);

    // set up environment for object storage
    TSFileDescriptor.getInstance()
        .getConfig()
        .setObjectStorageFile("com.timecho.iotdb.os.fileSystem.OSFile");
    TSFileDescriptor.getInstance()
        .getConfig()
        .setObjectStorageTsFileInput("com.timecho.iotdb.os.fileSystem.OSTsFileInput");
    TSFileDescriptor.getInstance()
        .getConfig()
        .setObjectStorageTsFileOutput("com.timecho.iotdb.os.fileSystem.OSTsFileOutput");
    // just to init the class
    //noinspection ResultOfMethodCallIgnored
    IPFilter.getAllowListPatterns();

    logger.info("IoTDB-DataNode environment variables: {}", IoTDBConfig.getEnvironmentVariables());
    logger.info("IoTDB-DataNode default charset is: {}", Charset.defaultCharset().displayName());
    DataNode dataNode = dataNodeSupplier.get();
    int returnCode = dataNode.run(args);
    if (returnCode != 0) {
      System.exit(returnCode);
    }
  }

  @Override
  protected void registerInternalRPCService() throws StartupException {
    // Start InternalRPCService to indicate that the current DataNode can accept cluster scheduling
    registerManager.register(DataNodeInternalRPCServiceNew.getInstance());
    registerManager.register(MigrationTaskManager.getInstance());
  }

  @Override
  protected String getClientRPCServiceImplClassName() {
    return ClientRPCServiceImplNew.class.getName();
  }

  @Override
  protected void versionCheck(TSystemConfigurationResp configurationResp) throws StartupException {
    if (!configurationResp.globalConfig.isEnterprise) {
      final String message =
          "TimechoDB DataNode can only be used with TimechoDB ConfigNode and cannot be used with IoTDB ConfigNode.";
      logger.error(message);
      throw new StartupException(message);
    }
  }

  @Override
  protected void saveSecretKey() {
    SecretKey.getInstance()
        .initSecretKeyFile(
            IoTDBDescriptor.getInstance().getConfig().getSystemDir(),
            String.valueOf(UUID.randomUUID()));
  }

  @Override
  protected void saveHardwareCode() {
    try {
      String hardwareCode = OSUtils.generateSystemInfoContentWithVersion();
      SecretKey.getInstance()
          .initHardwareCodeFile(
              IoTDBDescriptor.getInstance().getConfig().getSystemDir(), hardwareCode);
    } catch (Exception e) {
      logger.error("hardware generation failed.");
    }
  }

  @Override
  protected void loadSecretKey() throws IOException {
    SecretKey.getInstance()
        .loadSecretKeyFromFile(IoTDBDescriptor.getInstance().getConfig().getSystemDir());
  }

  @Override
  protected void loadHardwareCode() throws IOException {
    SecretKey.getInstance()
        .loadHardwareCodeFromFile(IoTDBDescriptor.getInstance().getConfig().getSystemDir());
  }

  private static class DataNodeNewHolder {

    private static final DataNode INSTANCE = new DataNode();

    private DataNodeNewHolder() {}
  }

  public static DataNode getInstance() {
    return DataNode.DataNodeNewHolder.INSTANCE;
  }
}
