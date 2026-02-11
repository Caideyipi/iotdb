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
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.platform.commons.logging.LoggerFactory;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBExternalServiceStartWithoutLib3IT extends IoTDBExternalServiceStartWithoutLibIT {

  @Before
  public void setUp() throws Exception {
    // enable both MQTT and REST service
    EnvFactory.getEnv().getConfig().getDataNodeConfig().setEnableMQTTService(true);
    EnvFactory.getEnv().getConfig().getDataNodeConfig().setEnableRestService(true);
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
}
