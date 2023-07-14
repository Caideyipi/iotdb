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

package org.apache.iotdb.db;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import com.timecho.iotdb.DataNode;
import com.timecho.iotdb.DataNodeServerCommandLineNew;
import com.timecho.iotdb.conf.TimechoDBDescriptor;
import com.timecho.iotdb.schemaregion.EnterpriseSchemaConstant;

import java.util.Properties;

/** This class is used to run integration test using timecho-server without license. */
public class HackTimechoServer extends DataNode {

  public static void main(String[] args) {
    Properties properties = TimechoDBDescriptor.getInstance().getCustomizedProperties();
    CommonDescriptor.getInstance().loadCommonProps(properties);
    IoTDBDescriptor.getInstance().loadProperties(properties);
    IoTDBDescriptor.getInstance().getConfig().setCustomizedProperties(properties);
    TSFileDescriptor.getInstance().overwriteConfigByCustomSettings(properties);
    TSFileDescriptor.getInstance().getConfig().setCustomizedProperties(properties);
    setUpEnterpriseEnvironment();
    new DataNodeServerCommandLineNew().doMain(args);
  }

  private static void setUpEnterpriseEnvironment() {
    // set up environment for schema region
    MNodeFactoryLoader.getInstance()
        .addScanPackage(EnterpriseSchemaConstant.ENTERPRISE_MNODE_FACTORY_PACKAGE);
    MNodeFactoryLoader.getInstance().setEnv(EnterpriseSchemaConstant.ENTERPRISE_MNODE_FACTORY_ENV);
  }
}
