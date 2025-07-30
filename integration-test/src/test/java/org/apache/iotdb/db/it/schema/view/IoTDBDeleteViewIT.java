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

package org.apache.iotdb.db.it.schema.view;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBDeleteViewIT extends AbstractSchemaIT {

  public IoTDBDeleteViewIT(final SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @Test
  public void deleteViewNormalTest() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeSeries root.turbine1.d1.s1 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute("CREATE VIEW root.turbine1.d1.s2 AS root.turbine1.d1.s1;");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1) VALUES(1,1)");
      statement.execute("flush");
      statement.execute("delete view root.turbine1.d1.s2");
      TestUtils.assertDataEventuallyOnEnv(
          EnvFactory.getEnv(),
          "count timeSeries root.turbine1.**",
          "count(timeseries),",
          Collections.singleton("1,"));
    }
  }

  @Test
  public void deleteViewByDeleteTimeSeriesTest() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeSeries root.turbine1.d1.s1 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute("CREATE VIEW root.turbine1.d1.s2 AS root.turbine1.d1.s1;");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1) VALUES(1,1)");
      statement.execute("flush");
      statement.execute("delete timeSeries root.turbine1.d1.s2");
      for (final DataNodeWrapper dataNodeWrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
        // Search the default data dir of data nodes
        try (final Stream<Path> filePaths =
            Files.walk(
                Paths.get(
                    dataNodeWrapper.getNodePath(),
                    IoTDBConstant.DN_DEFAULT_DATA_DIR
                        + File.separator
                        + IoTDBConstant.DATA_FOLDER_NAME))) {
          // Delete view by delete time series shall not generate any mods
          filePaths
              .filter(path -> path.toString().endsWith(ModificationFile.FILE_SUFFIX))
              .findFirst()
              .ifPresent(path -> Assert.fail());
        }
      }

      final AtomicBoolean hasMod = new AtomicBoolean(false);
      statement.execute("delete timeSeries root.turbine1.d1.s1");
      for (final DataNodeWrapper dataNodeWrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
        // Search the default data dir of data nodes
        try (final Stream<Path> filePaths =
            Files.walk(
                Paths.get(
                    dataNodeWrapper.getNodePath(),
                    IoTDBConstant.DN_DEFAULT_DATA_DIR
                        + File.separator
                        + IoTDBConstant.DATA_FOLDER_NAME))) {
          filePaths
              .filter(path -> path.toString().endsWith(ModificationFile.FILE_SUFFIX))
              .findFirst()
              .ifPresent(path -> hasMod.set(true));
          if (hasMod.get()) {
            return;
          }
        }
      }
      // If deleting normal time series does not generate any mods, the test needs to be altered to
      // satisfy the new feature
      Assert.fail();
    }
  }
}
