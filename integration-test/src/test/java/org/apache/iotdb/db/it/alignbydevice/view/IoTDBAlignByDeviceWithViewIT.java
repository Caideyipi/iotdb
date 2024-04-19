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

package org.apache.iotdb.db.it.alignbydevice.view;

import org.apache.iotdb.db.it.utils.AlignedWriteUtil;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.tsfile.utils.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlignByDeviceWithViewIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();

    AlignedWriteUtil.insertData();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : VIEW_SQL_LIST) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Execute view sql meets error: " + e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRawQuery() {
    String[] expectedRetArray =
        new String[] {
          "3,root.sg1.d1,30000.0,",
          "5,root.sg1.d1,5.0,",
          "6,root.sg1.d1,6.0,",
          "7,root.sg1.d1,7.0,",
          "8,root.sg1.d1,8.0,",
          "9,root.sg1.d1,9.0",
          "5,root.sg1.d2,5.0,",
          "6,root.sg1.d2,6.0,",
          "7,root.sg1.d2,7.0,",
          "8,root.sg1.d2,8.0,",
          "9,root.sg1.d2,9.0",
          "3,root.sg1.d3,30000.0,",
          "5,root.sg1.d3,5.0,",
          "6,root.sg1.d3,6.0,",
          "7,root.sg1.d3,7.0,",
          "8,root.sg1.d3,8.0,",
          "9,root.sg1.d3,9.0",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT s1 FROM root.sg1.** WHERE time<10 and s1>4 ALIGN BY DEVICE")) {
        checkResult(
            resultSet,
            expectedRetArray,
            "Time,Device,s1",
            new int[] {Types.TIMESTAMP, Types.VARCHAR, Types.FLOAT});
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAggregationQuery() {
    String[] expectedRetArray =
        new String[] {
          "3,root.sg1.d1,30000.0",
          "5,root.sg1.d1,6.0",
          "7,root.sg1.d1,8.0",
          "9,root.sg1.d1,9.0",
          "5,root.sg1.d2,6.0",
          "7,root.sg1.d2,8.0",
          "9,root.sg1.d2,9.0",
          "3,root.sg1.d3,30000.0",
          "5,root.sg1.d3,6.0",
          "7,root.sg1.d3,8.0",
          "9,root.sg1.d3,9.0",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT MAX_VALUE(s1) FROM root.sg1.** "
                  + "WHERE time<10 and s1>4 GROUP BY ([1, 20), 2ms) having(sum(s1)>6) ALIGN BY DEVICE")) {
        checkResult(
            resultSet,
            expectedRetArray,
            "Time,Device,MAX_VALUE(s1)",
            new int[] {Types.TIMESTAMP, Types.VARCHAR, Types.FLOAT});
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testLastQuery() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("SELECT last s1 FROM root.sg1.**")) {

        while (resultSet.next()) {
          String time = resultSet.getString(1);
          String name = resultSet.getString(2);
          String value = resultSet.getString(3);
          if ("root.sg1.d1.s1".equalsIgnoreCase(name)) {
            assertEquals("23", time);
            assertEquals("230000.0", value);
          } else if ("root.sg1.d2.s1".equalsIgnoreCase(name)) {
            assertEquals("20", time);
            assertEquals("20.0", value);
          } else if ("root.sg1.d3.s1".equalsIgnoreCase(name)) {
            assertEquals("23", time);
            assertEquals("230000.0", value);
          } else {
            fail("Unexpected series name: " + name);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testOrderBy() {
    String[] expectedHeader = new String[] {"Time,Device,vs1,vs2"};
    String[] retArray =
        new String[] {
          "2,root.view.d1,2.0,2,",
          "2,root.view.d2,2.0,2,",
          "3,root.view.d1,30000.0,null,",
          "3,root.view.d2,3.0,null,",
          "4,root.view.d1,4.0,4,"
        };
    resultSetEqualTest(
        "SELECT vs1, vs2 FROM root.view.** ORDER BY TIME OFFSET 2 LIMIT 5 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Time,Device,vs1,vs2"};
    retArray =
        new String[] {
          "39,root.view.d1,null,39,",
          "39,root.view.d2,null,39,",
          "38,root.view.d1,null,38,",
          "38,root.view.d2,null,38,",
          "37,root.view.d1,null,37,"
        };
    resultSetEqualTest(
        "SELECT vs1, vs2 FROM root.view.** ORDER BY TIME DESC OFFSET 2 LIMIT 5 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Device,count(vs1),first_value(vs2)"};
    retArray =
        new String[] {
          "root.view.d2,19,1,",
        };
    resultSetEqualTest(
        "SELECT count(vs1), first_value(vs2) FROM root.view.** ORDER BY TIME DESC OFFSET 1 LIMIT 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Time,Device,count(vs1),first_value(vs2)"};
    retArray =
        new String[] {
          "16,root.view.d1,5,12,", "16,root.view.d2,5,12,", "11,root.view.d1,4,7,",
        };
    resultSetEqualTest(
        "SELECT count(vs1), first_value(vs2) FROM root.view.** group by ((1,20], 5ms) ORDER BY TIME DESC OFFSET 2 LIMIT 3 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  @Test
  public void testErrorSqlList() {
    for (Pair<String, String> errorPair : ERROR_SQL_LIST) {
      String sql = errorPair.getLeft();
      String errorMsg = errorPair.getRight();

      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute(String.format(sql));
        Assert.fail(String.format("SQL [%s] should fail but no exception thrown.", sql));
      } catch (SQLException e) {
        Assert.assertEquals("701: " + errorMsg, e.getMessage());
      }
    }
  }

  private void checkResult(
      ResultSet resultSet, String[] retArray, String expectedHeaderStrings, int[] headerTypes)
      throws SQLException {

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    List<Integer> actualIndexToExpectedIndexList =
        checkHeader(resultSetMetaData, expectedHeaderStrings, headerTypes);

    int cnt = 0;
    while (resultSet.next()) {
      String[] expectedStrings = retArray[cnt].split(",");
      StringBuilder expectedBuilder = new StringBuilder();
      StringBuilder actualBuilder = new StringBuilder();
      for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
        actualBuilder.append(resultSet.getString(i)).append(",");
        expectedBuilder
            .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
            .append(",");
      }
      Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
      cnt++;
    }
    Assert.assertEquals(retArray.length, cnt);
  }

  private List<Integer> checkHeader(
      ResultSetMetaData resultSetMetaData, String expectedHeaderStrings, int[] expectedTypes)
      throws SQLException {
    String[] expectedHeaders = expectedHeaderStrings.split(",");
    Map<String, Integer> expectedHeaderToTypeIndexMap = new HashMap<>();
    for (int i = 0; i < expectedHeaders.length; ++i) {
      expectedHeaderToTypeIndexMap.put(expectedHeaders[i], i);
    }

    List<Integer> actualIndexToExpectedIndexList = new ArrayList<>();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      Integer typeIndex = expectedHeaderToTypeIndexMap.get(resultSetMetaData.getColumnName(i));
      Assert.assertNotNull(typeIndex);
      Assert.assertEquals(expectedTypes[typeIndex], resultSetMetaData.getColumnType(i));
      actualIndexToExpectedIndexList.add(typeIndex);
    }
    return actualIndexToExpectedIndexList;
  }

  private static final String[] VIEW_SQL_LIST =
      new String[] {
        "CREATE VIEW root.view.d1.vs1 as root.sg1.d1.s1;",
        "CREATE VIEW root.view.d1.vs2 as root.sg1.d1.s2;",
        "CREATE VIEW root.view.d2.vs1 as root.sg1.d2.s1;",
        "CREATE VIEW root.view.d2.vs2 as root.sg1.d2.s2;",
        "CREATE VIEW root.sg1.d3.s1 as root.sg1.d1.s1;",
        "CREATE VIEW root.sg1.d3.vs1 as root.sg1.d2.s1;",
        "CREATE VIEW root.sg1.d3.vs2 as root.sg1.d2.s1;",
        "CREATE VIEW root.sg1.d3.vs3 as select s2/2 from root.sg1.d1;"
      };

  private static final String VIEW_SAME_SERIES_ERROR =
      "Views or measurement aliases representing the same data source "
          + "cannot be queried concurrently in ALIGN BY DEVICE queries.";
  private static final String VIEW_IS_NOT_WRITABLE_ERROR =
      "Only writable view timeseries are supported in ALIGN BY DEVICE queries.";

  private static final List<Pair<String, String>> ERROR_SQL_LIST =
      Arrays.asList(
          new Pair<>("SELECT vs1, vs2 FROM root.sg1.** ALIGN BY DEVICE", VIEW_SAME_SERIES_ERROR),
          new Pair<>(
              "SELECT count(vs1), count(vs2) FROM root.sg1.** ALIGN BY DEVICE",
              VIEW_SAME_SERIES_ERROR),
          new Pair<>("SELECT vs3 FROM root.sg1.** ALIGN BY DEVICE", VIEW_IS_NOT_WRITABLE_ERROR),
          new Pair<>(
              "SELECT count(vs3) FROM root.sg1.** ALIGN BY DEVICE", VIEW_IS_NOT_WRITABLE_ERROR));
}
