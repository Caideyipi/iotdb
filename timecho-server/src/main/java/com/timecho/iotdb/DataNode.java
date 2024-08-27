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
import org.apache.iotdb.db.service.DataNodeInternalRPCService;
import org.apache.iotdb.db.service.RPCService;

import com.timecho.iotdb.schemaregion.EnterpriseSchemaConstant;
import com.timecho.iotdb.service.ClientRPCServiceImplNew;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

public class DataNode extends org.apache.iotdb.db.service.DataNode {
  private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

  public static void main(String[] args) {
    // set up environment for schema region
    MNodeFactoryLoader.getInstance()
        .addScanPackage(EnterpriseSchemaConstant.ENTERPRISE_MNODE_FACTORY_PACKAGE);
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

    logger.info("IoTDB-DataNode environment variables: {}", IoTDBConfig.getEnvironmentVariables());
    logger.info("IoTDB-DataNode default charset is: {}", Charset.defaultCharset().displayName());
    DataNode dataNode = new DataNode();
    int returnCode = dataNode.run(args);
    if (returnCode != 0) {
      System.exit(returnCode);
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

  @Override
  protected void versionCheck(TSystemConfigurationResp configurationResp) throws StartupException {
    if (!configurationResp.globalConfig.isEnterprise) {
      final String message = "Timecho DataNode cannot use with open source ConfigNode";
      logger.error(message);
      throw new StartupException(message);
    }
  }

  private static class DataNodeNewHolder {

    private static final DataNode INSTANCE = new DataNode();

    private DataNodeNewHolder() {}
  }

  public static DataNode getInstance() {
    return DataNode.DataNodeNewHolder.INSTANCE;
  }
}
