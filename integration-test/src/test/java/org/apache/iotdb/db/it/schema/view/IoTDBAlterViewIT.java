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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlterViewIT extends AbstractSchemaIT {

  public IoTDBAlterViewIT(SchemaTestMode schemaTestMode) {
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
  public void testAlterView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.db.d1.s1 with datatype=INT32");
      statement.execute("create timeseries root.db.d1.s2 with datatype=INT32");
      statement.execute("create timeseries root.db.d1.s3 with datatype=INT32");
      statement.execute("create timeseries root.db.d2.s1 with datatype=INT32");
      statement.execute("create timeseries root.db.d2.s2 with datatype=INT32");
      statement.execute("create timeseries root.db.d2.s3 with datatype=INT32");

      statement.execute(
          "create view root(view.d1.s1, view.d1.s2, view.d1.s3, view.d2.s1, view.d2.s2, view.d2.s3) "
              + "as root(db.d1.s1, db.d1.s2, db.d1.s3, db.d2.s1, db.d2.s2, db.d2.s3)");

      String[][] map =
          new String[][] {
            new String[] {"root.view.d1.s1", "root.db.d1.s1"},
            new String[] {"root.view.d1.s2", "root.db.d1.s2"},
            new String[] {"root.view.d1.s3", "root.db.d1.s3"},
            new String[] {"root.view.d2.s1", "root.db.d2.s1"},
            new String[] {"root.view.d2.s2", "root.db.d2.s2"},
            new String[] {"root.view.d2.s3", "root.db.d2.s3"},
          };
      for (String[] strings : map) {
        try (ResultSet resultSet =
            statement.executeQuery(String.format("show view %s", strings[0]))) {
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(strings[1], resultSet.getString("Source"));
        }
      }

      statement.execute(
          "alter view root(view.d1.s1, view.d1.s2, view.d1.s3) "
              + "as select s2,s3,s1 from root.db.d2");
      statement.execute(
          "alter view root(view.d2.s1, view.d2.s2, view.d2.s3) " + "as root.db.d1(s2,s3,s1)");

      map =
          new String[][] {
            new String[] {"root.view.d1.s1", "root.db.d2.s2"},
            new String[] {"root.view.d1.s2", "root.db.d2.s3"},
            new String[] {"root.view.d1.s3", "root.db.d2.s1"},
            new String[] {"root.view.d2.s1", "root.db.d1.s2"},
            new String[] {"root.view.d2.s2", "root.db.d1.s3"},
            new String[] {"root.view.d2.s3", "root.db.d1.s1"},
          };
      for (String[] strings : map) {
        try (ResultSet resultSet =
            statement.executeQuery(String.format("show view %s", strings[0]))) {
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(strings[1], resultSet.getString("Source"));
        }
      }
    }
  }

  @Test
  public void testAlterViewException() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.db.d1.s1 with datatype=INT32");
      statement.execute("create timeseries root.db.d1.s2 with datatype=INT32");
      statement.execute("create timeseries root.db.d1.s3 with datatype=INT32");
      statement.execute(
          "create view root(view.d1.s1, view.d1.s2, view.d1.s3) " + "as root.db.d1(s1, s2, s3)");
      try {
        statement.execute("alter view root.view.d1.s1 as root.db.d2.s1");
        Assert.fail("Exception expected");
      } catch (Exception e) {
        System.out.println(e.getMessage());
        Assert.assertTrue(
            e.getMessage().contains("Can not create a view based on non-exist time series"));
      }
      try {
        statement.execute("alter view root.view.d1.s1 as select s1 from root.db.d2");
        Assert.fail("Exception expected");
      } catch (Exception e) {
        System.out.println(e.getMessage());
        Assert.assertTrue(e.getMessage().contains("Columns in the query statement is empty."));
      }
      try {
        statement.execute("alter view root.view.d1.s1 as select s1,s2 from root.db.d1");
        Assert.fail("Exception expected");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("The number of target paths (1) and sources (2) are miss matched!"));
      }
    }
  }

  @Test
  public void testDeleteAndDropView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.db.d1.s1 with datatype=INT32");
      statement.execute("create timeseries root.db.d1.s2 with datatype=INT32");
      statement.execute("create view root(view.d1.s1, view.d1.s2) as root(db.d1.s1, db.d1.s2)");
      statement.execute(
          "insert into root.view.d1(time, s1, s2) values (1, 1, 10), (2, 2, 20), (1689579454282, 3, 30)");
      int[] result1 = new int[] {1, 2, 3};
      int[] result2 = new int[] {10, 20, 30};
      try (ResultSet resultSet = statement.executeQuery("select * from root.view.**")) {
        int index = 0;
        while (resultSet.next()) {
          int s1 = resultSet.getInt("root.view.d1.s1");
          int s2 = resultSet.getInt("root.view.d1.s2");
          assertEquals(result1[index], s1);
          assertEquals(result2[index], s2);
          index++;
        }
        assertEquals(3, index);
      }

      statement.execute("delete from root.view.d1.s2");

      int[] result = new int[] {1, 2, 3};
      try (ResultSet resultSet = statement.executeQuery("select * from root.view.**")) {
        int index = 0;
        while (resultSet.next()) {
          int s1 = resultSet.getInt("root.view.d1.s1");
          String s2 = resultSet.getString("root.view.d1.s2");
          assertEquals(result[index], s1);
          assertNull(s2);
          index++;
        }
        assertEquals(3, index);
      }

      statement.execute("drop view root.view.d1.s1");

      String[][] map =
          new String[][] {
            new String[] {"root.view.d1.s2", "root.db.d1.s2"},
          };
      for (String[] strings : map) {
        try (ResultSet resultSet =
            statement.executeQuery(String.format("show view %s", strings[0]))) {
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(strings[1], resultSet.getString("Source"));
        }
      }
    }
  }

  @Test
  public void testAlterViewTagsAndAttributes() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.db.d1.s1 with datatype=INT32");
      statement.execute("create timeseries root.db.d1.s2 with datatype=INT32");
      statement.execute("create timeseries root.db.d1.s3 with datatype=INT32");

      statement.execute("create view root.view.${2}(${3}) as select * from root.db.**;");

      String[][] map =
          new String[][] {
            new String[] {"root.view.d1.s1", "root.db.d1.s1"},
            new String[] {"root.view.d1.s2", "root.db.d1.s2"},
            new String[] {"root.view.d1.s3", "root.db.d1.s3"},
          };
      for (String[] strings : map) {
        try (ResultSet resultSet =
            statement.executeQuery(String.format("show view %s", strings[0]))) {
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(strings[1], resultSet.getString("Source"));
        }
      }

      // case1: ADD TAGS AND ATTRIBUES
      statement.execute("alter view root.view.d1.s1 ADD TAGS tag1=v1, tag2=v2");
      statement.execute("alter view root.view.d1.s1 ADD ATTRIBUTES attr1=a1");
      statement.execute("alter view root.view.d1.s2 ADD TAGS tag1=v1, tag2=v2");
      statement.execute("alter view root.view.d1.s2 ADD ATTRIBUTES attr2=a2");
      statement.execute("alter view root.view.d1.s3 ADD TAGS tag1=v1, tag2=v2");
      statement.execute("alter view root.view.d1.s3 ADD ATTRIBUTES attr3=a3");
      Set<String> expectedResult =
          new HashSet<>(
              Arrays.asList(
                  "root.view.d1.s1,INT32,{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr1\":\"a1\"},VIEW,root.db.d1.s1",
                  "root.view.d1.s2,INT32,{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"a2\"},VIEW,root.db.d1.s2",
                  "root.view.d1.s3,INT32,{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr3\":\"a3\"},VIEW,root.db.d1.s3"));
      try (ResultSet resultSet = statement.executeQuery("show view")) {
        while (resultSet.next()) {
          String actualResult =
              resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.TAGS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.ATTRIBUTES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.VIEW_TYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.SOURCE);
          Assert.assertTrue(expectedResult.contains(actualResult));
          expectedResult.remove(actualResult);
        }
      }
      Assert.assertTrue(expectedResult.isEmpty());

      // case2: DROP TAGS AND ATTRIBUES
      statement.execute("alter view root.view.d1.s1 DROP tag1");
      statement.execute("alter view root.view.d1.s2 DROP tag2");
      statement.execute("alter view root.view.d1.s3 DROP attr3");
      expectedResult =
          new HashSet<>(
              Arrays.asList(
                  "root.view.d1.s1,INT32,{\"tag2\":\"v2\"},{\"attr1\":\"a1\"},VIEW,root.db.d1.s1",
                  "root.view.d1.s2,INT32,{\"tag1\":\"v1\"},{\"attr2\":\"a2\"},VIEW,root.db.d1.s2",
                  "root.view.d1.s3,INT32,{\"tag1\":\"v1\",\"tag2\":\"v2\"},null,VIEW,root.db.d1.s3"));
      try (ResultSet resultSet = statement.executeQuery("show view")) {
        while (resultSet.next()) {
          String actualResult =
              resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.TAGS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.ATTRIBUTES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.VIEW_TYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.SOURCE);
          Assert.assertTrue(expectedResult.contains(actualResult));
          expectedResult.remove(actualResult);
        }
      }
      Assert.assertTrue(expectedResult.isEmpty());

      // case3: SET TAGS AND ATTRIBUES
      statement.execute("alter view root.view.d1.s1 SET tag2=new2");
      statement.execute("alter view root.view.d1.s2 SET attr2=new2");
      expectedResult =
          new HashSet<>(
              Arrays.asList(
                  "root.view.d1.s1,INT32,{\"tag2\":\"new2\"},{\"attr1\":\"a1\"},VIEW,root.db.d1.s1",
                  "root.view.d1.s2,INT32,{\"tag1\":\"v1\"},{\"attr2\":\"new2\"},VIEW,root.db.d1.s2",
                  "root.view.d1.s3,INT32,{\"tag1\":\"v1\",\"tag2\":\"v2\"},null,VIEW,root.db.d1.s3"));
      try (ResultSet resultSet = statement.executeQuery("show view")) {
        while (resultSet.next()) {
          String actualResult =
              resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.TAGS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.ATTRIBUTES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.VIEW_TYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.SOURCE);
          Assert.assertTrue(expectedResult.contains(actualResult));
          expectedResult.remove(actualResult);
        }
      }
      Assert.assertTrue(expectedResult.isEmpty());

      // case4: RENAME TAGS AND ATTRIBUES
      statement.execute("alter view root.view.d1.s1 RENAME tag2 TO newTag2");
      statement.execute("alter view root.view.d1.s2 RENAME attr2 TO newAttr2");
      expectedResult =
          new HashSet<>(
              Arrays.asList(
                  "root.view.d1.s1,INT32,{\"newTag2\":\"new2\"},{\"attr1\":\"a1\"},VIEW,root.db.d1.s1",
                  "root.view.d1.s2,INT32,{\"tag1\":\"v1\"},{\"newAttr2\":\"new2\"},VIEW,root.db.d1.s2",
                  "root.view.d1.s3,INT32,{\"tag1\":\"v1\",\"tag2\":\"v2\"},null,VIEW,root.db.d1.s3"));
      try (ResultSet resultSet = statement.executeQuery("show view")) {
        while (resultSet.next()) {
          String actualResult =
              resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.TAGS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.ATTRIBUTES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.VIEW_TYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.SOURCE);
          Assert.assertTrue(expectedResult.contains(actualResult));
          expectedResult.remove(actualResult);
        }
      }
      Assert.assertTrue(expectedResult.isEmpty());
    }
  }

  @Test
  public void testQueryFilterByTag() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.db.d1.s1 with datatype=INT32");
      statement.execute("create timeseries root.db.d1.s2 with datatype=INT32");
      statement.execute("create timeseries root.db.d1.s3 with datatype=INT32");

      statement.execute("create view root.view.${2}(${3}) as select * from root.db.**;");

      String[][] map =
          new String[][] {
            new String[] {"root.view.d1.s1", "root.db.d1.s1"},
            new String[] {"root.view.d1.s2", "root.db.d1.s2"},
            new String[] {"root.view.d1.s3", "root.db.d1.s3"},
          };
      for (String[] strings : map) {
        try (ResultSet resultSet =
            statement.executeQuery(String.format("show view %s", strings[0]))) {
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(strings[1], resultSet.getString("Source"));
        }
      }

      // ADD TAGS
      statement.execute("alter view root.view.d1.s1 ADD TAGS tag1=v1");
      statement.execute("alter view root.view.d1.s2 ADD TAGS tag1=v2");
      statement.execute("alter view root.view.d1.s3 ADD TAGS tag1=v1");
      Set<String> expectedResult =
          new HashSet<>(
              Arrays.asList(
                  "root.view.d1.s1,INT32,{\"tag1\":\"v1\"},null,VIEW,root.db.d1.s1",
                  "root.view.d1.s2,INT32,{\"tag1\":\"v2\"},null,VIEW,root.db.d1.s2",
                  "root.view.d1.s3,INT32,{\"tag1\":\"v1\"},null,VIEW,root.db.d1.s3"));
      try (ResultSet resultSet = statement.executeQuery("show view")) {
        while (resultSet.next()) {
          String actualResult =
              resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.TAGS)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.ATTRIBUTES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.VIEW_TYPE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.SOURCE);
          Assert.assertTrue(expectedResult.contains(actualResult));
          expectedResult.remove(actualResult);
        }
      }
      Assert.assertTrue(expectedResult.isEmpty());

      // case1: query where TAGS(tag1)='v1'
      expectedResult = new HashSet<>(Arrays.asList("root.view.d1.s1", "root.view.d1.s3"));
      try (ResultSet resultSet = statement.executeQuery("show view where TAGS(tag1)='v1';")) {
        while (resultSet.next()) {
          String actualResult = resultSet.getString(ColumnHeaderConstant.TIMESERIES);
          Assert.assertTrue(expectedResult.contains(actualResult));
          expectedResult.remove(actualResult);
        }
      }
      Assert.assertTrue(expectedResult.isEmpty());

      // case2: query where TAGS(tag1) contains 'v'
      expectedResult =
          new HashSet<>(Arrays.asList("root.view.d1.s1", "root.view.d1.s2", "root.view.d1.s3"));
      try (ResultSet resultSet =
          statement.executeQuery("show view where TAGS(tag1) contains 'v';")) {
        while (resultSet.next()) {
          String actualResult = resultSet.getString(ColumnHeaderConstant.TIMESERIES);
          Assert.assertTrue(expectedResult.contains(actualResult));
          expectedResult.remove(actualResult);
        }
      }
      Assert.assertTrue(expectedResult.isEmpty());
    }
  }

  @Test
  public void testUpsertAliasException() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.db.d1.s1 with datatype=INT32");
      statement.execute("create view root.view.d1.s1 as root.db.d1.s1;");
      try {
        statement.execute("alter view root.view.d1.s1 upsert alias=a;");
        Assert.fail("expect exception");
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("View doesn't support alias"));
      }
    }
  }

  @Test
  public void testAlterViewTagAttributesException() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.db.d1.s1 with datatype=INT32");
      statement.execute("create view root.view.d1.s1 as root.db.d1.s1;");
      try {
        statement.execute("alter view root.view.d1.s1 add tags t1=a,t1=b;");
        Assert.fail("expect exception");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage().contains("There's duplicate [t1] in tag or attribute clause."));
      }
      statement.execute("alter view root.view.d1.s1 add tags t1=a,t2=b;");
      try {
        statement.execute("alter view root.view.d1.s1 set t1=a,t1=b;");
        Assert.fail("expect exception");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage().contains("There's duplicate [t1] in tag or attribute clause."));
      }
      try {
        statement.execute("alter view root.view.d1.s1 upsert tags(t1=a,t1=b);");
        Assert.fail("expect exception");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage().contains("There's duplicate [t1] in tag or attribute clause."));
      }
      try {
        statement.execute("alter view root.view.d1.s1 rename t2 to t1;");
        Assert.fail("expect exception");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("TimeSeries [root.view.d1.s1] already has a tag/attribute named [t1]"));
      }
    }
  }
}
