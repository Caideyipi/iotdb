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

package org.apache.iotdb.confignode.it.cluster;

import org.apache.iotdb.commons.exception.LicenseException;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterRestartFileEncryptIT {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBClusterRestartFileEncryptIT.class);
  private static final int testConfigNodeNum = 1, testDataNodeNum = 1;

  @Before
  public void setUp() throws LicenseException {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSeriesSlotNum(50)
        .setEnableEncryptConfigFile(true)
        .setEnableEncryptPermissionFile(true);

    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void userPrivilegeFileClusterRestartTest() {
    userAndRoleGenerator();
    testSetConfiguration();
    testRejectSetConfiguration();

    // Shutdown all cluster nodes
    logger.info("Shutting down all ConfigNodes and DataNodes...");
    EnvFactory.getEnv().shutdownAllConfigNodes();
    EnvFactory.getEnv().shutdownAllDataNodes();

    // Restart all cluster nodes
    logger.info("Restarting all ConfigNodes...");
    EnvFactory.getEnv().startAllConfigNodes();
    logger.info("Restarting all DataNodes...");
    EnvFactory.getEnv().startAllDataNodes();
    ((AbstractEnv) EnvFactory.getEnv()).checkClusterStatusWithoutUnknown();

    Assert.assertTrue(checkConfigFileContains("series_slot_num=50"));
    Assert.assertTrue(checkConfigFileContains("enable_query_memory_estimation=false"));
    userAndRoleCheck();
  }

  public void userAndRoleGenerator() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create database test");
      statement.execute("create user test1 'password123456'");
      statement.execute("create role role1");
      statement.execute("grant role role1 to test1");
      statement.execute("GRANT CREATE ON DATABASE test TO USER test1 WITH GRANT OPTION");
      statement.execute("USE test");
      statement.execute("CREATE TABLE table1(id int32)");
      statement.execute("INSERT INTO table1 (id) VALUES (1)");
      statement.execute("GRANT SELECT ON ANY TO ROLE role1");
      ResultSet resultSet = statement.executeQuery("SELECT * FROM table1");
      outputResult(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void userAndRoleCheck() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("LIST USER");
      String ans = "0,root,-1,1,\n" + "10000,test1,-1,-1,\n";
      validateResultSet(resultSet, ans);
      resultSet = statement.executeQuery("LIST ROLE");
      ans = "role1,\n";
      validateResultSet(resultSet, ans);
      resultSet = statement.executeQuery("LIST ROLE OF USER test1");
      ans = "role1,\n";
      validateResultSet(resultSet, ans);
      resultSet = statement.executeQuery("LIST PRIVILEGES OF USER test1");
      ans = ",test.*,CREATE,true,\n" + "role1,*.*,SELECT,false,\n";
      validateResultSet(resultSet, ans);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static void outputResult(ResultSet resultSet) throws SQLException {
    if (resultSet != null) {
      System.out.println("--------------------------");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int columnCount = metaData.getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        System.out.print(metaData.getColumnLabel(i + 1) + " ");
      }
      System.out.println();
      while (resultSet.next()) {
        for (int i = 1; ; i++) {
          System.out.print(resultSet.getString(i));
          if (i < columnCount) {
            System.out.print(", ");
          } else {
            System.out.println();
            break;
          }
        }
      }
      System.out.println("--------------------------\n");
    }
  }

  public static void validateResultSet(ResultSet set, String ans) throws SQLException {
    try {
      StringBuilder builder = new StringBuilder();
      ResultSetMetaData metaData = set.getMetaData();
      int colNum = metaData.getColumnCount();
      while (set.next()) {
        for (int i = 1; i <= colNum; i++) {
          builder.append(set.getString(i)).append(",");
        }
        builder.append("\n");
      }
      String result = builder.toString();
      assertEquals(ans.length(), result.length());
      List<String> ansLines = Arrays.asList(ans.split("\n"));
      List<String> resultLines = Arrays.asList(result.split("\n"));
      assertEquals(ansLines.size(), resultLines.size());
      for (String resultLine : resultLines) {
        assertTrue(ansLines.contains(resultLine));
      }
    } finally {
      set.close();
    }
  }

  private static boolean checkConfigFileContains(String... contents) {
    Map<String, String> showConfigurationResults = new HashMap<>();
    for (String content : contents) {
      String[] split = content.split("=");
      showConfigurationResults.put(split[0], split[1]);
    }
    return checkShowConfigurationContains(showConfigurationResults);
  }

  private static boolean checkShowConfigurationContains(Map<String, String> expectedKeyValues) {
    try (ITableSession tableSessionConnection = EnvFactory.getEnv().getTableSessionConnection()) {
      SessionDataSet sessionDataSet =
          tableSessionConnection.executeQueryStatement("show configuration");
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        String name = iterator.getString(1);
        String value = iterator.isNull(2) ? null : iterator.getString(2);
        String expectedValue = expectedKeyValues.remove(name);
        if (expectedValue != null && !expectedValue.equals(value)) {
          return false;
        }
      }
    } catch (Exception e) {
      return false;
    }
    return expectedKeyValues.isEmpty();
  }

  public void testSetConfiguration() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"enable_query_memory_estimation\"=\"false\"");
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  public void testRejectSetConfiguration() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"enable_encrypt_config_file\"=\"false\"");
    } catch (Exception e) {
      assertEquals(
          "803: No permission to turn off the switch of encrypt config file", e.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"enable_encrypt_permission_file\"=\"false\"");
    } catch (Exception e) {
      assertEquals(
          "803: No permission to turn off the switch of encrypt permission file", e.getMessage());
    }
  }
}
