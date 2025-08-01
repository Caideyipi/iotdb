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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.itbase.constant.TestConstant.count;
import static org.junit.Assert.fail;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBDeleteTimeSeriesIT extends AbstractSchemaIT {

  public IoTDBDeleteTimeSeriesIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  @Test
  public void deleteTimeSeriesAndCreateDifferentTypeTest() throws Exception {
    String[] retArray = new String[] {"1,1,", "2,1.1,"};
    int cnt = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine1.d1.s2 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1,s2) VALUES(1,1,2)");

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.turbine1.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
      statement.execute("DELETE timeseries root.turbine1.d1.s1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=DOUBLE, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1) VALUES(2,1.1)");
      statement.execute("FLUSH");

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.turbine1.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
    }

    //    EnvironmentUtils.restartDaemon();
    //
    //    try (Connection connection = EnvFactory.getEnv().getConnection();
    //        Statement statement = connection.createStatement()) {
    //      boolean hasResult = statement.execute("SELECT * FROM root.**");
    //      Assert.assertTrue(hasResult);
    //    }

  }

  @Test
  public void deleteTimeSeriesAndCreateSameTypeTest() throws Exception {
    String[] retArray = new String[] {"1,1,", "2,5,"};
    int cnt = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine1.d1.s2 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1,s2) VALUES(1,1,2)");

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.turbine1.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
      statement.execute("DELETE timeseries root.turbine1.d1.s1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1) VALUES(2,5)");
      statement.execute("FLUSH");

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.turbine1.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
    }

    //    EnvironmentUtils.restartDaemon();
    //
    //    try (Connection connection = EnvFactory.getEnv().getConnection();
    //        Statement statement = connection.createStatement()) {
    //      boolean hasResult = statement.execute("SELECT * FROM root.**");
    //      Assert.assertTrue(hasResult);
    //    }
  }

  @Test
  public void deleteTimeSeriesMultiIntervalTest() {
    String[] retArray1 = new String[] {"0"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String insertSql = "insert into root.sg.d1(time, s1) values(%d, %d)";
      for (int i = 1; i <= 4; i++) {
        statement.execute(String.format(insertSql, i, i));
      }
      statement.execute("flush");

      statement.execute("delete from root.sg.d1.s1 where time >= 1 and time <= 2");
      statement.execute("delete from root.sg.d1.s1 where time >= 3 and time <= 4");

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1) from root.sg.d1 where time >= 3 and time <= 4")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(count("root.sg.d1.s1"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void deleteTimeSeriesAndAutoDeleteDeviceTest() throws Exception {
    String[] retArray1 = new String[] {"4,4,4,4"};

    String insertSql = "insert into root.sg.d1(time, s1, s2, s3, s4) values(%d, %d, %d, %d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= 4; i++) {
        statement.execute(String.format(insertSql, i, i, i, i, i));
      }

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), count(s2), count(s3), count(s4) from root.sg.d1")) {
        while (resultSet.next()) {
          StringBuilder ans = new StringBuilder(resultSet.getString(count("root.sg.d1.s1")));
          for (int i = 2; i <= 4; i++) {
            ans.append(",").append(resultSet.getString(count("root.sg.d1.s" + i)));
          }
          Assert.assertEquals(retArray1[cnt], ans.toString());
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      statement.execute("delete timeseries root.sg.d1.*");
      try (ResultSet resultSet = statement.executeQuery("select * from root.sg.d1")) {
        Assert.assertFalse(resultSet.next());
      }

      try (ResultSet resultSet = statement.executeQuery("show timeseries root.sg.d1.*")) {
        Assert.assertFalse(resultSet.next());
      }

      try (ResultSet resultSet = statement.executeQuery("show devices root.sg.d1")) {
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void deleteTimeSeriesAndInvalidationTest() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("insert into root.sg.d1 (c1, c2) values (1, 1)");
      statement.execute("delete timeSeries root.sg.d1.**");
      try (final ResultSet resultSet = statement.executeQuery("select c1, c2 from root.sg.d1")) {
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void deleteTimeSeriesCrossSchemaRegionTest() throws Exception {
    String[] retArray1 = new String[] {"4,4,4,4"};

    String insertSql = "insert into root.sg.d%d(time, s1, s2) values(%d, %d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= 4; i++) {
        for (int j = 1; j <= 4; j++) {
          statement.execute(String.format(insertSql, j, i, i, i));
        }
      }

      int cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("select count(s1) from root.sg.*")) {
        while (resultSet.next()) {
          StringBuilder ans = new StringBuilder(resultSet.getString(count("root.sg.d1.s1")));
          for (int i = 2; i <= 4; i++) {
            ans.append(",").append(resultSet.getString(count("root.sg.d" + i + ".s1")));
          }
          Assert.assertEquals(retArray1[cnt], ans.toString());
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      statement.execute("delete timeseries root.sg.*.s1");
      try (ResultSet resultSet = statement.executeQuery("select s1 from root.sg.*")) {
        Assert.assertFalse(resultSet.next());
      }

      try (ResultSet resultSet = statement.executeQuery("show timeseries root.sg.*.s1")) {
        Assert.assertFalse(resultSet.next());
      }

      cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("select count(s2) from root.sg.*")) {
        while (resultSet.next()) {
          StringBuilder ans = new StringBuilder(resultSet.getString(count("root.sg.d1.s2")));
          for (int i = 2; i <= 4; i++) {
            ans.append(",").append(resultSet.getString(count("root.sg.d" + i + ".s2")));
          }
          Assert.assertEquals(retArray1[cnt], ans.toString());
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    }
  }

  @Test
  public void deleteTimeSeriesCrossStorageGroupTest() throws Exception {
    String[] retArray1 = new String[] {"4,4,4,4"};

    String insertSql = "insert into root.sg%d.d1(time, s1, s2) values(%d, %d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= 4; i++) {
        for (int j = 1; j <= 4; j++) {
          statement.execute(String.format(insertSql, j, i, i, i));
        }
      }

      int cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("select count(s1) from root.*.d1")) {
        while (resultSet.next()) {
          StringBuilder ans = new StringBuilder(resultSet.getString(count("root.sg1.d1.s1")));
          for (int i = 2; i <= 4; i++) {
            ans.append(",").append(resultSet.getString(count("root.sg" + i + ".d1.s1")));
          }
          Assert.assertEquals(retArray1[cnt], ans.toString());
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      statement.execute("delete timeseries root.*.d1.s1");
      try (ResultSet resultSet = statement.executeQuery("select s1 from root.*.*")) {
        Assert.assertFalse(resultSet.next());
      }

      try (ResultSet resultSet = statement.executeQuery("show timeseries root.*.*.s1")) {
        Assert.assertFalse(resultSet.next());
      }

      cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("select count(s2) from root.*.*")) {
        while (resultSet.next()) {
          StringBuilder ans = new StringBuilder(resultSet.getString(count("root.sg1.d1.s2")));
          for (int i = 2; i <= 4; i++) {
            ans.append(",").append(resultSet.getString(count("root.sg" + i + ".d1.s2")));
          }
          Assert.assertEquals(retArray1[cnt], ans.toString());
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      statement.execute("delete timeseries root.sg1.d1.s2, root.sg2.**");
      try (ResultSet resultSet = statement.executeQuery("select s2 from root.sg1.*")) {
        Assert.assertFalse(resultSet.next());
      }

      try (ResultSet resultSet = statement.executeQuery("show timeseries root.sg2.*.s2")) {
        Assert.assertFalse(resultSet.next());
      }

      retArray1 = new String[] {"4,4"};
      cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("select count(s2) from root.*.*")) {
        while (resultSet.next()) {
          StringBuilder ans = new StringBuilder(resultSet.getString(count("root.sg3.d1.s2")));
          for (int i = 4; i <= 4; i++) {
            ans.append(",").append(resultSet.getString(count("root.sg" + i + ".d1.s2")));
          }
          Assert.assertEquals(retArray1[cnt], ans.toString());
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    }
  }

  @Test
  public void deleteTimeSeriesWithMultiPatternTest() throws Exception {
    String[] retArray1 = new String[] {"4,4,4,4"};

    String insertSql = "insert into root.sg%d.d1(time, s1, s2) values(%d, %d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= 4; i++) {
        for (int j = 1; j <= 4; j++) {
          statement.execute(String.format(insertSql, j, i, i, i));
        }
      }

      int cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("select count(s1) from root.*.d1")) {
        while (resultSet.next()) {
          StringBuilder ans = new StringBuilder(resultSet.getString(count("root.sg1.d1.s1")));
          for (int i = 2; i <= 4; i++) {
            ans.append(",").append(resultSet.getString(count("root.sg" + i + ".d1.s1")));
          }
          Assert.assertEquals(retArray1[cnt], ans.toString());
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      statement.execute("delete timeseries root.*.d1.s1, root.*.d1.s2");
      try (ResultSet resultSet = statement.executeQuery("select * from root.*.*")) {
        Assert.assertFalse(resultSet.next());
      }

      try (ResultSet resultSet = statement.executeQuery("show timeseries root.*.*.*")) {
        Assert.assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void deleteTemplateTimeSeriesTest() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.db");
      statement.execute("CREATE DEVICE TEMPLATE t1 (s1 INT64, s2 DOUBLE)");
      statement.execute("SET DEVICE TEMPLATE t1 to root.db");
      statement.execute("CREATE TIMESERIES USING DEVICE TEMPLATE ON root.db.d1");
      try {
        statement.execute("DELETE TIMESERIES root.db.**");
        Assert.fail();
      } catch (SQLException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    TSStatusCode.PATH_NOT_EXIST.getStatusCode()
                        + ": Timeseries [root.db.**] does not exist or is represented by device template"));
      }
      try {
        statement.execute("DELETE TIMESERIES root.db.**");
        Assert.fail();
      } catch (SQLException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    TSStatusCode.PATH_NOT_EXIST.getStatusCode()
                        + ": Timeseries [root.db.**] does not exist or is represented by device template"));
      }
    }
  }

  @Test
  public void deleteTimeSeriesAndReturnPathNotExistsTest() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("delete timeseries root.db.**");
        Assert.fail();
      } catch (SQLException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    TSStatusCode.PATH_NOT_EXIST.getStatusCode()
                        + ": Timeseries [root.db.**] does not exist or is represented by device template"));
      }

      String[] retArray1 = new String[] {"4,4,4,4"};

      String insertSql = "insert into root.sg%d.d1(time, s1, s2) values(%d, %d, %d)";
      for (int i = 1; i <= 4; i++) {
        for (int j = 1; j <= 4; j++) {
          statement.execute(String.format(insertSql, j, i, i, i));
        }
      }

      int cnt = 0;

      try (ResultSet resultSet = statement.executeQuery("select count(s1) from root.*.d1")) {
        while (resultSet.next()) {
          StringBuilder ans = new StringBuilder(resultSet.getString(count("root.sg1.d1.s1")));
          for (int i = 2; i <= 4; i++) {
            ans.append(",").append(resultSet.getString(count("root.sg" + i + ".d1.s1")));
          }
          Assert.assertEquals(retArray1[cnt], ans.toString());
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      try {
        statement.execute("delete timeseries root.*.d1.s3");
        Assert.fail();
      } catch (SQLException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    TSStatusCode.PATH_NOT_EXIST.getStatusCode()
                        + ": Timeseries [root.*.d1.s3] does not exist or is represented by device template"));
      }
    }
  }

  @Test
  public void dropTimeSeriesTest() throws Exception {
    String[] retArray = new String[] {"1,1,", "2,1.1,"};
    int cnt = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine1.d1.s2 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1,s2) VALUES(1,1,2)");

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.turbine1.d1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
      statement.execute("DROP timeseries root.turbine1.d1.s1");
      statement.execute("FLUSH");

      try (ResultSet resultSet = statement.executeQuery("show timeseries root.turbine1.d1")) {
        Assert.assertFalse(resultSet.next());
      }
    }
  }
}
