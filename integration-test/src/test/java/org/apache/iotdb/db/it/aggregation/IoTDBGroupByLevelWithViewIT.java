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

package org.apache.iotdb.db.it.aggregation;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBGroupByLevelWithViewIT {

  private static final String[] SQLs =
      new String[] {
        "create database root.db;",
        "create timeseries root.db.beijing.d1.s1 INT32;",
        "create timeseries root.db.beijing.d1.s2 INT32;",
        "insert into root.db.beijing.d1(time, s1, s2) values (1, 1, 1);",
        "insert into root.db.beijing.d1(time, s2) values (2, 2);",
        "insert into root.db.beijing.d1(time, s1) values (3, 3);",
        "create aligned timeseries root.db.shanghai.d1(s1 INT32, s2 INT32);",
        "insert into root.db.shanghai.d1(time, s1, s2) values (2, 2, 2);",
        "insert into root.db.shanghai.d1(time, s1) values (3, 3);",
        "insert into root.db.shanghai.d1(time, s2) values (4, 5);",
        "create database root.view;",
        "create view root.view.d1.s1 as root.db.beijing.d1.s1;",
        "create view root.view.d1.s2 as root.db.beijing.d1.s2;",
        "create view root.view.d2.s1 as root.db.shanghai.d1.s1;",
        "create view root.view.d2.s2 as root.db.shanghai.d1.s2;",
        "create view root.view.d3.s1 as select s1 + s2 from root.db.shanghai.d1;",
        "create view root.view.d3.s2 as select shanghai.d1.s1 + beijing.d1.s2 from root.db;"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSumLevel0() {
    String expectedHeader =
        "sum(root.*.*.*.s1),sum(root.*.*.s1),sum(root.*.*.*.s2),sum(root.*.*.s2),";
    String[] retArray = new String[] {"9.0,13.0,10.0,14.0,"};

    resultSetEqualTest(
        "select sum(s1), sum(s2) from root.** group by level = 0;", expectedHeader, retArray);
  }

  @Test
  public void testSumLevel1() {
    String expectedHeader =
        "sum(root.db.*.*.s1),sum(root.view.*.s1),sum(root.db.*.*.s2),sum(root.view.*.s2),";
    String[] retArray = new String[] {"9.0,13.0,10.0,14.0,"};

    resultSetEqualTest(
        "select sum(s1), sum(s2) from root.** group by level = 1;", expectedHeader, retArray);
  }

  @Test
  public void testSumLevel2() {
    String expectedHeader =
        "sum(root.*.shanghai.*.s1),sum(root.*.beijing.*.s1),sum(root.*.d1.s1),sum(root.*.d2.s1),"
            + "sum(root.*.d3.s1),sum(root.*.shanghai.*.s2),sum(root.*.beijing.*.s2),"
            + "sum(root.*.d1.s2),sum(root.*.d2.s2),sum(root.*.d3.s2),";
    String[] retArray = new String[] {"5.0,4.0,4.0,5.0,4.0,7.0,3.0,3.0,7.0,4.0,"};

    resultSetEqualTest(
        "select sum(s1), sum(s2) from root.** group by level = 2;", expectedHeader, retArray);
  }

  @Test
  public void testCountStarLevel0() {
    String expectedHeader = "count(root.*.*.*.*),count(root.*.*.*),";
    String[] retArray = new String[] {"9,10,"};

    resultSetEqualTest(
        "select count(*) from root.** group by level = 0;", expectedHeader, retArray);
  }

  @Test
  public void testCountStarLevel1() {
    String expectedHeader = "count(root.__system.*.*.*),count(root.db.*.*.*),count(root.view.*.*),";
    String[] retArray = new String[] {"1,8,10,"};

    resultSetEqualTest(
        "select count(*) from root.** group by level = 1;", expectedHeader, retArray);
  }

  @Test
  public void testCountStarLevel2() {
    String expectedHeader =
        "count(root.*.password_history.*.*),count(root.*.shanghai.*.*),count(root.*.beijing.*.*),count(root.*.d1.*),count(root.*.d2.*),count(root.*.d3.*),";
    String[] retArray = new String[] {"1,4,4,4,4,2,"};

    resultSetEqualTest(
        "select count(*) from root.** group by level = 2;", expectedHeader, retArray);
  }

  @Test
  public void testExpression1() {
    String expectedHeader =
        "count(root.*.*.*.s1) + sum(root.*.*.*.s2),count(root.*.*.*.s1) + sum(root.*.*.s2),"
            + "count(root.*.*.s1) + sum(root.*.*.*.s2),count(root.*.*.s1) + sum(root.*.*.s2),";
    String[] retArray = new String[] {"14.0,18.0,15.0,19.0,"};

    resultSetEqualTest(
        "select count(s1) + sum(s2) from root.** group by level = 0;", expectedHeader, retArray);
  }

  @Test
  public void testExpression2() {
    String expectedHeader =
        "sum(root.*.*.*.s1 + root.*.*.*.s2),sum(root.*.*.*.s1 + root.*.*.s2),"
            + "sum(root.*.*.s1 + root.*.*.*.s2),sum(root.*.*.s1 + root.*.*.s2),";
    String[] retArray = new String[] {"10.0,16.0,22.0,36.0,"};

    resultSetEqualTest(
        "select sum(s1 + s2) from root.** group by level = 0;", expectedHeader, retArray);
  }

  @Test
  public void testGroupByTimeLevel0() {
    String expectedHeader =
        "Time,sum(root.*.*.*.s1),sum(root.*.*.s1),sum(root.*.*.*.s2),sum(root.*.*.s2),";
    String[] retArray =
        new String[] {
          "0,null,null,null,null,",
          "1,1.0,1.0,1.0,1.0,",
          "2,2.0,6.0,4.0,8.0,",
          "3,6.0,6.0,null,null,",
          "4,null,null,5.0,5.0,"
        };

    resultSetEqualTest(
        "select sum(s1), sum(s2) from root.** group by ([0, 5), 1ms), level = 0;",
        expectedHeader,
        retArray);
  }

  @Test
  public void testGroupByTimeLevel1() {
    String expectedHeader =
        "Time,sum(root.db.*.*.s1),sum(root.view.*.s1),sum(root.db.*.*.s2),sum(root.view.*.s2),";
    String[] retArray =
        new String[] {
          "0,null,null,null,null,",
          "1,1.0,1.0,1.0,1.0,",
          "2,2.0,6.0,4.0,8.0,",
          "3,6.0,6.0,null,null,",
          "4,null,null,5.0,5.0,"
        };

    resultSetEqualTest(
        "select sum(s1), sum(s2) from root.** group by ([0, 5), 1ms), level = 1;",
        expectedHeader,
        retArray);
  }

  @Test
  public void testGroupByTimeLevel2() {
    String expectedHeader =
        "Time,sum(root.*.shanghai.*.s1),sum(root.*.beijing.*.s1),sum(root.*.d1.s1),sum(root.*.d2.s1),"
            + "sum(root.*.d3.s1),sum(root.*.shanghai.*.s2),sum(root.*.beijing.*.s2),"
            + "sum(root.*.d1.s2),sum(root.*.d2.s2),sum(root.*.d3.s2),";
    String[] retArray =
        new String[] {
          "0,null,null,null,null,null,null,null,null,null,null,",
          "1,null,1.0,1.0,null,null,null,1.0,1.0,null,null,",
          "2,2.0,null,null,2.0,4.0,2.0,2.0,2.0,2.0,4.0,",
          "3,3.0,3.0,3.0,3.0,null,null,null,null,null,null,",
          "4,null,null,null,null,null,5.0,null,null,5.0,null,"
        };

    resultSetEqualTest(
        "select sum(s1), sum(s2) from root.** group by ([0, 5), 1ms), level = 2;",
        expectedHeader,
        retArray);
  }

  @Test
  public void testHaving1() {
    String expectedHeader =
        "Time,sum(root.*.*.*.s1),sum(root.*.*.s1),sum(root.*.*.*.s2),sum(root.*.*.s2),";
    String[] retArray = new String[] {"3,6.0,6.0,null,null,"};

    resultSetEqualTest(
        "select sum(s1), sum(s2) from root.** group by ([0, 5), 1ms), level = 0 having sum(s1) > 2;",
        expectedHeader,
        retArray);
  }

  @Test
  public void testHaving2() {
    String expectedHeader =
        "Time,sum(root.*.*.*.s1),sum(root.*.*.s1),sum(root.*.*.*.s2),sum(root.*.*.s2),";
    String[] retArray = new String[] {"2,2.0,6.0,4.0,8.0,"};

    resultSetEqualTest(
        "select sum(s1), sum(s2) from root.** group by ([0, 5), 1ms), level = 0 having sum(s1) + sum(s2) > 4;",
        expectedHeader,
        retArray);
  }

  @Test
  public void testOrderBy() {
    String expectedHeader = "Time,sum(root.*.*.s1),";
    String[] retArray = new String[] {"1,1.0,", "2,6.0,", "3,6.0,", "0,null,", "4,null,"};

    resultSetEqualTest(
        "select sum(s1) from root.view.** group by ([0, 5), 1ms), level = 0 order by sum(s1);",
        expectedHeader,
        retArray);
  }

  @Test
  public void testOrderByDesc() {
    String expectedHeader = "Time,sum(root.*.*.s1),";
    String[] retArray = new String[] {"2,6.0,", "3,6.0,", "1,1.0,", "0,null,", "4,null,"};

    resultSetEqualTest(
        "select sum(s1) from root.view.** group by ([0, 5), 1ms), level = 0 order by sum(s1) desc;",
        expectedHeader,
        retArray);
  }

  @Test
  public void testCaseSensitivity() {
    String expectedHeader =
        "sum(root.*.*.*.s1),sum(root.*.*.s1),sUm(root.*.*.*.s2),sUm(root.*.*.s2),SUM(root.*.*.*.s2),SUM(root.*.*.s2),";
    String[] retArray = new String[] {"9.0,13.0,10.0,14.0,10.0,14.0,"};

    resultSetEqualTest(
        "select sum(s1), sUm(s2), SUM(s2) from root.** group by level = 0;",
        expectedHeader,
        retArray);
  }

  @Test
  public void testSlimitSoffset() {
    String expectedHeader = "sum(root.*.*.s1),sUm(root.*.*.*.s2),";
    String[] retArray = new String[] {"13.0,10.0,"};

    resultSetEqualTest(
        "select sum(s1), sUm(s2), SUM(s2) from root.** group by level = 0 slimit 2 soffset 1;",
        expectedHeader,
        retArray);
  }

  @Test
  public void testSameColumn1() {
    String expectedHeader =
        "sum(root.db.*.*.s1),sum(root.view.*.s1),sum(root.db.*.*.s1),sum(root.view.*.s1),";
    String[] retArray = new String[] {"9.0,13.0,9.0,13.0,"};

    resultSetEqualTest(
        "select sum(s1), sum(s1) from root.** group by level = 1;", expectedHeader, retArray);
  }

  @Test
  public void testSameColumn2() {
    String expectedHeader =
        "count(root.__system.*.*.*),count(root.db.*.*.*),count(root.view.*.*),count(root.__system.*.*.*),count(root.db.*.*.*),count(root.view.*.*),";
    String[] retArray = new String[] {"1,8,10,1,8,10,"};

    resultSetEqualTest(
        "select count(*), count(*) from root.** group by level = 1;", expectedHeader, retArray);
  }
}
