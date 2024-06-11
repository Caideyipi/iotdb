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

package org.apache.iotdb.session.it;

import org.apache.iotdb.common.rpc.thrift.TShowConfigurationResp;
import org.apache.iotdb.common.rpc.thrift.TShowConfigurationTemplateResp;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import com.timecho.iotdb.isession.pool.ISessionPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBShowConfigurationIT {

  @BeforeClass
  public static void setUpClass() throws IOException {
    System.setProperty("TestEnv", "Timecho");
  }

  @Before
  public void setUp() {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testShowConfiguration()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    try (com.timecho.iotdb.isession.ISession session =
        (com.timecho.iotdb.isession.ISession) EnvFactory.getEnv().getSessionConnection()) {
      TShowConfigurationTemplateResp showConfigurationTemplateResp =
          session.showConfigurationTemplate();
      Assert.assertTrue(
          showConfigurationTemplateResp != null
              && !showConfigurationTemplateResp.getContent().isEmpty());
      TShowConfigurationResp showConfigurationResp = session.showConfiguration(0);
      Assert.assertTrue(
          showConfigurationResp != null && !showConfigurationResp.getContent().isEmpty());
      Assert.assertEquals(
          getConfigurationContent(EnvFactory.getEnv().getConfigNodeWrapper(0)),
          showConfigurationResp.getContent());
      showConfigurationResp = session.showConfiguration(1);
      Assert.assertTrue(
          showConfigurationResp != null && !showConfigurationResp.getContent().isEmpty());
      Assert.assertEquals(
          getConfigurationContent(EnvFactory.getEnv().getDataNodeWrapper(0)),
          showConfigurationResp.getContent());
    }
  }

  @Test
  public void testShowConfigurationWithSessionPool()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    ISessionPool sessionPool = (ISessionPool) EnvFactory.getEnv().getSessionPool(3);
    try {
      TShowConfigurationTemplateResp showConfigurationTemplateResp =
          sessionPool.showConfigurationTemplate();
      Assert.assertTrue(
          showConfigurationTemplateResp != null
              && !showConfigurationTemplateResp.getContent().isEmpty());
      TShowConfigurationResp showConfigurationResp = sessionPool.showConfiguration(0);
      Assert.assertTrue(
          showConfigurationResp != null && !showConfigurationResp.getContent().isEmpty());
      Assert.assertEquals(
          getConfigurationContent(EnvFactory.getEnv().getConfigNodeWrapper(0)),
          showConfigurationResp.getContent());
      showConfigurationResp = sessionPool.showConfiguration(1);
      Assert.assertTrue(
          showConfigurationResp != null && !showConfigurationResp.getContent().isEmpty());
      Assert.assertEquals(
          getConfigurationContent(EnvFactory.getEnv().getDataNodeWrapper(0)),
          showConfigurationResp.getContent());
    } finally {
      sessionPool.close();
    }
  }

  private String getConfigurationContent(AbstractNodeWrapper nodeWrapper) throws IOException {
    String systemPropertiesPath =
        nodeWrapper.getNodePath()
            + File.separator
            + "conf"
            + File.separator
            + CommonConfig.SYSTEM_CONFIG_NAME;
    File f = new File(systemPropertiesPath);
    return new String(Files.readAllBytes(f.toPath()));
  }
}
