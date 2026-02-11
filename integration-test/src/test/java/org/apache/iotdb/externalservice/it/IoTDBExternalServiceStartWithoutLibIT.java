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

package org.apache.iotdb.externalservice.it;

import org.apache.iotdb.db.service.externalservice.BuiltinExternalServices;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.platform.commons.logging.LoggerFactory;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBExternalServiceStartWithoutLibIT {
  protected DataNodeWrapper dataNodeWrapper;
  protected String serviceName;

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getDataNodeConfig().setEnableMQTTService(true);
    try {
      EnvFactory.getEnv().initClusterEnvironment(1, 1, 60);
    } catch (AssertionError error) {
      // The DN should fail because less of related jar
      LoggerFactory.getLogger(IoTDBExternalServiceStartWithoutLibIT.class)
          .warn(() -> error.getMessage());
      dataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapperList().get(0);
      serviceName = BuiltinExternalServices.MQTT.getServiceName();
      return;
    }
    fail();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test() throws IOException {
    assertTrue(
        dataNodeWrapper.logContains(
            String.format(
                "Failed to start External Service %s, because its instance can not be constructed successfully",
                serviceName)));
    assertTrue(dataNodeWrapper.logContains("DataNode exits"));
  }
}
