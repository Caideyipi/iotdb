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

package org.apache.iotdb.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBDeletionWithAliasAndInvalidIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setPartitionInterval(1000)
        .setMemtableSizeThreshold(10000);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * Test delete data with normal series, alias series, and invalid series. Setup: - root.sg1.d1.s1:
   * normal series - root.sg1.d1.s2: alias series (renamed from root.sg1.d1.s2_physical) -
   * root.sg1.d1.s3: invalid series (disabled) - root.sg1.d1.s4: normal series
   */
  @Test
  public void testDeleteDataWithAliasAndInvalidSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s3_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg1.d1.s4 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data for all series
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg1.d1(timestamp, s1, s2_physical, s3_physical, s4) VALUES(%d, %d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3, i * 4));
      }

      // Create alias series: rename s2_physical to s2_alias
      statement.execute("ALTER TIMESERIES root.sg1.d1.s2_physical RENAME TO root.sg1.d1.s2_alias");
      statement.execute("ALTER TIMESERIES root.sg1.d1.s3_physical RENAME TO root.sg2.d1.s3_alias");

      // Verify data exists before deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Test 1: Delete from normal series s1
      statement.execute("DELETE FROM root.sg1.d1.s1 WHERE time <= 50");
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // Only 5 rows should remain (60-100)
      }

      // Test 2: Delete from alias series s2_alias (should delete from physical series s2_physical)
      statement.execute("DELETE FROM root.sg1.d1.s2_alias WHERE time <= 60");
      try (ResultSet resultSet = statement.executeQuery("SELECT s2_physical FROM root.sg1.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s2_alias FROM root.sg1.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(4, count); // Only 4 rows should remain (70-100)
      }

      statement.execute("DELETE FROM root.sg1.d1.s3_alias WHERE time <= 100");
      // If deletion succeeds, verify no data was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s3_alias FROM root.sg1.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }

      // Test 4: Delete from normal series s4 (should work normally)
      statement.execute("DELETE FROM root.sg1.d1.s4 WHERE time <= 70");
      try (ResultSet resultSet = statement.executeQuery("SELECT s4 FROM root.sg1.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(3, count); // Only 3 rows should remain (80-100)
      }

      // Test 5: Delete from multiple series including alias and invalid
      statement.execute(
          "DELETE FROM root.sg1.d1.s1, root.sg1.d1.s2_alias, root.sg1.d1.s3_alias WHERE time <= 80");
      // s1 and s2_alias should be deleted, s3_alias should be skipped
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(2, count); // Only 2 rows should remain (90-100)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with pattern matching that includes alias and invalid series. */
  @Test
  public void testDeleteDataWithPatternIncludingAliasAndInvalid() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create multiple timeseries
      statement.execute("CREATE TIMESERIES root.sg2.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg2.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg2.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg2.d1.s4 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg2.d1(timestamp, s1, s2_physical, s3, s4) VALUES(%d, %d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3, i * 4));
      }

      // Create alias series
      statement.execute("ALTER TIMESERIES root.sg2.d1.s2_physical RENAME TO root.sg2.d1.s2_alias");

      // Test: Delete from pattern ** (should handle alias and invalid series)
      statement.execute("DELETE FROM root.sg2.d1.** WHERE time <= 50");

      // Verify deletion results
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg2.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain
      }

      // Verify physical path
      try (ResultSet resultSet = statement.executeQuery("SELECT s2_physical FROM root.sg2.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }

      // Verify alias path
      try (ResultSet resultSet = statement.executeQuery("SELECT s2_alias FROM root.sg2.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data when all series are invalid - should throw exception. */
  @Test
  public void testDeleteDataAllInvalidSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute("CREATE TIMESERIES root.sg3.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg3.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg3.d1(timestamp, s1, s2) VALUES(%d, %d, %d)", i * 10, i, i * 2));
      }

      statement.execute("ALTER TIMESERIES root.sg3.d1.s1 RENAME TO root.db4.d1.s1");
      statement.execute("ALTER TIMESERIES root.sg3.d1.s2 RENAME TO root.db4.d1.s2");

      try {
        statement.execute("DELETE FROM root.sg3.d1.** WHERE time <= 100");
        Assert.fail("Delete data all invalid series should fail");
      } catch (SQLException e) {
        // Expected exception when all series are invalid
        assertTrue(
            e.getMessage().contains("invalid")
                || e.getMessage().contains("disabled")
                || e.getMessage().contains("all target series"));
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with mixed scenario: normal, alias, and invalid series. */
  @Test
  public void testDeleteDataMixedScenario() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute("CREATE TIMESERIES root.sg4.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg4.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg4.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg4.d1.s4 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg4.d1.s5 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 20; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg4.d1(timestamp, s1, s2_physical, s3, s4, s5) VALUES(%d, %d, %d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3, i * 4, i * 5));
      }

      // Create alias series: s2_physical -> s2_alias
      statement.execute("ALTER TIMESERIES root.sg4.d1.s2_physical RENAME TO root.sg4.d1.s2_alias");

      // Verify initial data
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM root.sg4.d1")) {
        assertTrue(resultSet.next());
        assertEquals(20, resultSet.getInt(1));
      }

      // Test: Delete with range that includes various types
      statement.execute(
          "DELETE FROM root.sg4.d1.s1, root.sg4.d1.s2_alias, root.sg4.d1.s3 WHERE time <= 100");

      // Verify s1 (normal) was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM root.sg4.d1")) {
        assertTrue(resultSet.next());
        assertEquals(10, resultSet.getInt(1)); // 10 rows remaining (110-200)
      }

      // Verify s2_physical
      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_physical) FROM root.sg4.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify s2_alias reflects deletion
      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_alias) FROM root.sg4.d1")) {
        assertTrue(resultSet.next());
        assertEquals(10, resultSet.getInt(1)); // 10 rows remaining (110-200)
      }

      // Test: Delete with wildcard pattern
      statement.execute("DELETE FROM root.sg4.d1.s* WHERE time <= 150");

      // Verify results after wildcard deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM root.sg4.d1")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1)); // Should have fewer rows
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with exact path matching for alias series. */
  @Test
  public void testDeleteDataExactPathAliasSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup
      statement.execute(
          "CREATE TIMESERIES root.sg5.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg5.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg5.d1(timestamp, s1_physical, s2_physical) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      // Create alias series
      statement.execute("ALTER TIMESERIES root.sg5.d1.s1_physical RENAME TO root.sg5.d1.s1_alias");
      statement.execute("ALTER TIMESERIES root.sg5.d1.s2_physical RENAME TO root.sg5.d1.s2_alias");

      // Test: Delete using alias path (should delete from physical path)
      statement.execute("DELETE FROM root.sg5.d1.s1_alias WHERE time <= 50");

      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s1_physical) FROM root.sg5.d1")) {
        assertFalse(resultSet.next());
      }

      try {
        statement.execute("DELETE FROM root.sg5.d1.s2_physical WHERE time <= 60");
        fail("Delete should throw exception");
      } catch (SQLException e) {
        e.printStackTrace();
        Assert.assertTrue(
            "701: Cannot delete data: all target series are invalid".equals(e.getMessage()));
      }

      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_physical) FROM root.sg5.d1")) {
        assertFalse(resultSet.next());
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with prefix pattern matching (e.g., root.sg.d1.s*). */
  @Test
  public void testDeleteDataWithPrefixPattern() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute("CREATE TIMESERIES root.sg6.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg6.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg6.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg6.d1.t1 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg6.d1(timestamp, s1, s2_physical, s3, t1) VALUES(%d, %d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3, i * 4));
      }

      // Create alias series
      statement.execute("ALTER TIMESERIES root.sg6.d1.s2_physical RENAME TO root.sg6.d1.s2_alias");

      // Test: Delete using prefix pattern s* (should match s1, s2_alias, s3 but not t1)
      statement.execute("DELETE FROM root.sg6.d1.s* WHERE time <= 50");

      // Verify s1 was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM root.sg6.d1")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_physical) FROM root.sg6.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify s2_alias reflects deletion
      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_alias) FROM root.sg6.d1")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      // Verify s3 was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s3) FROM root.sg6.d1")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      // Verify t1 was NOT deleted (doesn't match pattern s*)
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(t1) FROM root.sg6.d1")) {
        assertTrue(resultSet.next());
        assertEquals(10, resultSet.getInt(1));
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with device prefix pattern (e.g., root.sg.d*.s1). */
  @Test
  public void testDeleteDataWithDevicePrefixPattern() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries in multiple devices
      statement.execute("CREATE TIMESERIES root.sg7.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg7.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg7.d2.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg7.d2.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg7.d1(timestamp, s1, s2_physical) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
        statement.execute(
            String.format(
                "INSERT INTO root.sg7.d2(timestamp, s1, s2_physical) VALUES(%d, %d, %d)",
                i * 10, i * 10, i * 20));
      }

      // Create alias series
      statement.execute("ALTER TIMESERIES root.sg7.d1.s2_physical RENAME TO root.sg7.d1.s2_alias");
      statement.execute("ALTER TIMESERIES root.sg7.d2.s2_physical RENAME TO root.sg7.d2.s2_alias");

      // Test: Delete using device prefix pattern **.s1
      statement.execute("DELETE FROM root.sg7.**.s1 WHERE time <= 50");

      // Verify d1.s1 was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM root.sg7.d1")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      // Verify d2.s1 was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM root.sg7.d2")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with logical view. */
  @Test
  public void testDeleteDataWithLogicalView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute("CREATE TIMESERIES root.sg8.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg8.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg8.d1(timestamp, s1, s2) VALUES(%d, %d, %d)", i * 10, i, i * 2));
      }

      // Create logical view
      statement.execute("CREATE VIEW root.sg8.d1.view1 AS root.sg8.d1.s1");

      // Test: Delete from source path (should affect view)
      statement.execute("DELETE FROM root.sg8.d1.s1 WHERE time <= 50");

      // Verify source path was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM root.sg8.d1")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      // Verify view reflects the deletion (view reads from s1)
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(view1) FROM root.sg8.d1")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with combination: normal + alias + invalid + view. */
  @Test
  public void testDeleteDataCombinedScenario() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute("CREATE TIMESERIES root.sg9.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg9.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg9.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg9.d1.s4 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 20; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg9.d1(timestamp, s1, s2_physical, s3, s4) VALUES(%d, %d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3, i * 4));
      }

      // Create alias series
      statement.execute("ALTER TIMESERIES root.sg9.d1.s2_physical RENAME TO root.sg9.d1.s2_alias");

      // Create logical view
      statement.execute("CREATE VIEW root.sg9.d1.view1 AS root.sg9.d1.s4");

      // Test: Delete using wildcard pattern ** (should handle all types)
      statement.execute("DELETE FROM root.sg9.d1.** WHERE time <= 100");

      // Verify s1 (normal) was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM root.sg9.d1")) {
        assertTrue(resultSet.next());
        assertEquals(10, resultSet.getInt(1));
      }

      // Verify s2_physical (physical path of alias)
      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_physical) FROM root.sg9.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify s2_alias reflects deletion
      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_alias) FROM root.sg9.d1")) {
        assertTrue(resultSet.next());
        assertEquals(10, resultSet.getInt(1));
      }

      // Verify s4 (source of view) was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s4) FROM root.sg9.d1")) {
        assertTrue(resultSet.next());
        assertEquals(10, resultSet.getInt(1));
      }

      // Verify view reflects the deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(view1) FROM root.sg9.d1")) {
        assertTrue(resultSet.next());
        assertEquals(10, resultSet.getInt(1));
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with prefix pattern including alias and invalid series. */
  @Test
  public void testDeleteDataPrefixPatternWithAliasAndInvalid() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute("CREATE TIMESERIES root.sg10.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg10.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg10.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg10.d1.s4 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg10.d1.t1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg10.d2.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg10.d2.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg10.d2.s3 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg10.d2.s4 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg10.d2.t1 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg10.d1(timestamp, s1, s2_physical, s3, s4, t1) VALUES(%d, %d, %d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3, i * 4, i * 5));
        statement.execute(
            String.format(
                "INSERT INTO root.sg10.d2(timestamp, s1, s2_physical, s3, s4, t1) VALUES(%d, %d, %d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3, i * 4, i * 5));
      }

      // Create alias series
      statement.execute(
          "ALTER TIMESERIES root.sg10.d1.s2_physical RENAME TO root.sg10.d1.s2_alias");
      statement.execute(
          "ALTER TIMESERIES root.sg10.d2.s2_physical RENAME TO root.sg10.d1.s5_alias");

      // Test: Delete using prefix pattern
      statement.execute("DELETE FROM root.sg10.d1.s* WHERE time <= 60");

      // Verify s1 was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM root.sg10.d1")) {
        assertTrue(resultSet.next());
        assertEquals(4, resultSet.getInt(1));
      }

      // Verify s2_physical
      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_physical) FROM root.sg10.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify s2_alias reflects deletion
      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_alias) FROM root.sg10.d1")) {
        assertTrue(resultSet.next());
        assertEquals(4, resultSet.getInt(1));
      }

      // Verify s4 was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s4) FROM root.sg10.d1")) {
        assertTrue(resultSet.next());
        assertEquals(4, resultSet.getInt(1));
      }

      // Verify s5_alias was deleted
      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s5_alias) FROM root.sg10.d1")) {
        assertTrue(resultSet.next());
        assertEquals(4, resultSet.getInt(1));
      }

      // Verify t1 was NOT deleted (doesn't match pattern s*)
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(t1) FROM root.sg10.d1")) {
        assertTrue(resultSet.next());
        assertEquals(10, resultSet.getInt(1));
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with multi-level wildcard pattern. */
  @Test
  public void testDeleteDataMultiLevelWildcard() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries in multiple levels
      statement.execute("CREATE TIMESERIES root.sg12.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg12.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg12.d2.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg12.d2.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg12.d1(timestamp, s1, s2_physical) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
        statement.execute(
            String.format(
                "INSERT INTO root.sg12.d2(timestamp, s1, s2_physical) VALUES(%d, %d, %d)",
                i * 10, i * 10, i * 20));
      }

      // Create alias series
      statement.execute(
          "ALTER TIMESERIES root.sg12.d1.s2_physical RENAME TO root.sg12.d1.s2_alias");
      statement.execute(
          "ALTER TIMESERIES root.sg12.d2.s2_physical RENAME TO root.sg12.d2.s2_alias");

      // Test: Delete using multi-level wildcard **
      statement.execute("DELETE FROM root.sg12.** WHERE time <= 50");

      // Verify all series were deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM root.sg12.d1")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_physical) FROM root.sg12.d1")) {
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_alias) FROM root.sg12.d1")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM root.sg12.d2")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_physical) FROM root.sg12.d2")) {
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_alias) FROM root.sg12.d2")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /**
   * Test delete data with complex combination: normal + alias + invalid + view + prefix pattern.
   */
  @Test
  public void testDeleteDataComplexCombination() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute("CREATE TIMESERIES root.sg13.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg13.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg13.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg13.d1.s4 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg13.d1.t1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg13.d2.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg13.d2.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg13.d1(timestamp, s1, s2_physical, s3, s4, t1) VALUES(%d, %d, %d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3, i * 4, i * 5));
        statement.execute(
            String.format(
                "INSERT INTO root.sg13.d2(timestamp, s1, s2_physical) VALUES(%d, %d, %d)",
                i * 10, i * 10, i * 20));
      }

      // Create alias series
      statement.execute(
          "ALTER TIMESERIES root.sg13.d1.s2_physical RENAME TO root.sg13.d1.s2_alias");
      statement.execute(
          "ALTER TIMESERIES root.sg13.d2.s2_physical RENAME TO root.sg13.d2.s2_alias");

      // Create logical view
      statement.execute("CREATE VIEW root.sg13.d1.view1 AS root.sg13.d1.s4");

      // Test: Delete using device prefix pattern d*.s* (should match s1, s2_alias, s3, s4 but not
      // t1)
      statement.execute("DELETE FROM root.sg13.d*.s* WHERE time <= 50");

      // Verify d1.s1 was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM root.sg13.d1")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_physical) FROM root.sg13.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify d1.s2_alias reflects deletion
      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_alias) FROM root.sg13.d1")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      // Verify d1.s4 (source of view) was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s4) FROM root.sg13.d1")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      // Verify d1.t1 was NOT deleted (doesn't match pattern s*)
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(t1) FROM root.sg13.d1")) {
        assertTrue(resultSet.next());
        assertEquals(10, resultSet.getInt(1));
      }

      // Verify d2.s1 was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(s1) FROM root.sg13.d2")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_physical) FROM root.sg13.d2")) {
        assertFalse(resultSet.next());
      }

      // Verify d2.s2_alias reflects deletion
      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(s2_alias) FROM root.sg13.d2")) {
        assertTrue(resultSet.next());
        assertEquals(5, resultSet.getInt(1));
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with alias series in different databases. */
  @Test
  public void testDeleteDataWithAliasInDifferentDatabases() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries in different databases
      statement.execute(
          "CREATE TIMESERIES root.db1.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.db2.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db1.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db2.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db1.d1(timestamp, s1_physical, s2) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
        statement.execute(
            String.format(
                "INSERT INTO root.db2.d1(timestamp, s1_physical, s2) VALUES(%d, %d, %d)",
                i * 10, i * 10, i * 20));
      }

      // Create alias series: rename from db1 to db2 (cross-database alias)
      statement.execute("ALTER TIMESERIES root.db1.d1.s1_physical RENAME TO root.db2.d1.s1_alias");

      // Verify alias exists
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.db2.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Test: Delete from alias series in db2 (should delete from physical path in db1)
      statement.execute("DELETE FROM root.db2.d1.s1_alias WHERE time <= 50");

      // Verify physical path in db1 cannot be queried (it's disabled after rename)
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db1.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count); // Physical path is disabled, should return 0
      }

      // Verify alias in db2 reflects deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.db2.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (60-100)
      }

      // Test: Delete from pattern that includes cross-database alias
      statement.execute("DELETE FROM root.db2.d1.** WHERE time <= 70");

      // Verify all data in db2.d1 was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.db2.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(3, count); // 3 rows should remain (80-100)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete timeseries with alias series. */
  @Test
  public void testDeleteTimeSeriesWithAlias() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute(
          "CREATE TIMESERIES root.sg14.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg14.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg14.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg14.d1(timestamp, s1_physical, s2_physical, s3) VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      // Create alias series
      statement.execute(
          "ALTER TIMESERIES root.sg14.d1.s1_physical RENAME TO root.sg14.d1.s1_alias");
      statement.execute(
          "ALTER TIMESERIES root.sg14.d1.s2_physical RENAME TO root.sg14.d1.s2_alias");

      // Verify alias exists
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Test 1: Delete timeseries using alias path (should delete physical path)
      statement.execute("DELETE TIMESERIES root.sg14.d1.s1_alias");
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }

      // Test 2: Delete timeseries using physical path (should fail)
      try {
        statement.execute("DELETE TIMESERIES root.sg14.d1.s2_physical");
        fail("Should throw exception when trying to delete physical path with alias");
      } catch (SQLException e) {
        // Expected: should throw exception when trying to delete physical path with alias
        assertTrue(
            e.getMessage().contains("alias")
                || e.getMessage().contains("renamed")
                || e.getMessage().contains("disable"));
      }

      // Test 3: Delete timeseries using pattern that includes alias
      statement.execute("DELETE TIMESERIES root.sg14.d1.s*");

      // Verify all s* series are deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s2_alias FROM root.sg14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s3 FROM root.sg14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }

      // Clean up
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete timeseries with invalid (disabled) series. */
  @Test
  public void testDeleteTimeSeriesWithInvalidSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute("CREATE TIMESERIES root.sg15.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg15.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg15.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg15.d1(timestamp, s1, s2, s3) VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      // Test: Delete timeseries with pattern (should handle invalid series)
      statement.execute("DELETE TIMESERIES root.sg15.d1.s1");

      // Verify s1 is deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg15.d1")) {
        assertFalse(resultSet.next());
      }

      // Test: Delete timeseries with pattern including invalid series
      statement.execute("DELETE TIMESERIES root.sg15.d1.s*");

      // Verify all s* series are deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s2 FROM root.sg15.d1")) {
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s3 FROM root.sg15.d1")) {
        assertFalse(resultSet.next());
      }
    }
  }

  /** Test delete database with alias series. */
  @Test
  public void testDeleteDatabaseWithAliasSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries in multiple databases
      statement.execute(
          "CREATE TIMESERIES root.db3.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db3.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.db4.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db4.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db3.d1(timestamp, s1_physical, s2) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
        statement.execute(
            String.format(
                "INSERT INTO root.db4.d1(timestamp, s1_physical, s2) VALUES(%d, %d, %d)",
                i * 10, i * 10, i * 20));
      }

      // Create alias series: rename within same database
      statement.execute("ALTER TIMESERIES root.db3.d1.s1_physical RENAME TO root.db3.d1.s1_alias");

      // Create alias series: rename across databases
      statement.execute("ALTER TIMESERIES root.db4.d1.s1_physical RENAME TO root.db3.d1.s1_cross");

      // Verify alias exists
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.db3.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s1_cross FROM root.db3.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Test 1: Delete database with alias series (same database)
      statement.execute("DELETE DATABASE root.db3");

      // Test 2: Delete database that is source of cross-database alias
      statement.execute("DELETE DATABASE root.db4");
    }
  }

  /** Test delete database with invalid (disabled) series. */
  @Test
  public void testDeleteDatabaseWithInvalidSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute("CREATE TIMESERIES root.db5.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db5.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db5.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("ALTER TIMESERIES root.db5.d1.s1 RENAME TO root.db5.d2.s1");
      statement.execute("ALTER TIMESERIES root.db5.d1.s2 RENAME TO root.db5.d2.s2");
      statement.execute("ALTER TIMESERIES root.db5.d1.s3 RENAME TO root.db5.d2.s3");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db5.d2(timestamp, s1, s2, s3) VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      try {
        statement.execute("DELETE DATABASE root.db5");
        // If succeeds, verify database is deleted
        try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.db5.d1")) {
          assertFalse(resultSet.next());
        }
      } catch (SQLException e) {
        fail(e.getMessage());
      }
    }
  }

  /** Test delete database with mixed scenario: normal + alias + invalid series. */
  @Test
  public void testDeleteDatabaseMixedScenario() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries in multiple databases
      statement.execute(
          "CREATE TIMESERIES root.db6.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db6.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db6.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.db7.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db6.d1(timestamp, s1_physical, s2, s3) VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
        statement.execute(
            String.format(
                "INSERT INTO root.db7.d1(timestamp, s1_physical) VALUES(%d, %d)", i * 10, i * 10));
      }

      // Create alias series: within same database
      statement.execute("ALTER TIMESERIES root.db6.d1.s1_physical RENAME TO root.db6.d1.s1_alias");

      // Create alias series: across databases
      statement.execute("ALTER TIMESERIES root.db7.d1.s1_physical RENAME TO root.db6.d1.s1_cross");

      statement.execute("DELETE DATABASE root.db6");

      // Clean up
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete timeseries with cross-database alias. */
  @Test
  public void testDeleteTimeSeriesWithCrossDatabaseAlias() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries in different databases
      statement.execute(
          "CREATE TIMESERIES root.db8.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db9.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db8.d1(timestamp, s1_physical) VALUES(%d, %d)", i * 10, i));
        statement.execute(
            String.format("INSERT INTO root.db9.d1(timestamp, s2) VALUES(%d, %d)", i * 10, i * 2));
      }

      // Create cross-database alias
      statement.execute("ALTER TIMESERIES root.db8.d1.s1_physical RENAME TO root.db9.d1.s1_alias");

      // Verify alias exists
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.db9.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Test: Delete timeseries using physical path (should fail if alias exists)
      try {
        statement.execute("DELETE TIMESERIES root.db8.d1.s1_physical");
        fail("Should throw exception when trying to delete physical path with alias");
      } catch (SQLException e) {
        // Expected: should throw exception when trying to delete physical path with alias
        assertTrue(
            e.getMessage().contains("alias")
                || e.getMessage().contains("renamed")
                || e.getMessage().contains("disable"));
      }

      // Test: Delete timeseries using alias path (should delete physical path)
      statement.execute("DELETE TIMESERIES root.db9.d1.s1_alias");

      // Verify physical path no longer exists
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db8.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify alias path no longer exists
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.db9.d1")) {
        assertFalse(resultSet.next());
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with alias series created from aligned series. */
  @Test
  public void testDeleteDataWithAliasFromAlignedSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create aligned timeseries
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.sg14.d1(s1_physical INT32, s2 INT32, s3 INT32, s4 INT32)");

      // Insert data for aligned series
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg14.d1(timestamp, s1_physical, s2, s3, s4) ALIGNED VALUES(%d, %d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3, i * 4));
      }

      // Verify aligned series exists
      try (ResultSet resultSet =
          statement.executeQuery("SELECT s1_physical, s2, s3, s4 FROM root.sg14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Create alias series from aligned series measurement
      statement.execute(
          "ALTER TIMESERIES root.sg14.d1.s1_physical RENAME TO root.sg14.d1.s1_alias");

      // Verify alias exists and aligned series still works
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Test 1: Delete data from alias series (should delete from aligned series)
      statement.execute("DELETE FROM root.sg14.d1.s1_alias WHERE time <= 50");

      // Verify alias data was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (60-100)
      }

      // Verify other aligned measurements are not affected
      try (ResultSet resultSet = statement.executeQuery("SELECT s2, s3, s4 FROM root.sg14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count); // All 10 rows should remain
      }

      // Test 2: Delete data from other aligned measurements
      statement.execute("DELETE FROM root.sg14.d1.s2 WHERE time <= 60");

      // Verify s2 was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s2 FROM root.sg14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(4, count); // 4 rows should remain (70-100)
      }

      // Test 3: Delete data using pattern that includes alias
      statement.execute("DELETE FROM root.sg14.d1.s* WHERE time <= 70");

      // Verify all s* series were deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(3, count); // 3 rows should remain (80-100)
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s3 FROM root.sg14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(3, count); // 3 rows should remain (80-100)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete timeseries with alias from aligned series. */
  @Test
  public void testDeleteTimeSeriesWithAliasFromAlignedSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create aligned timeseries
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.sg15.d1(s1_physical INT32, s2 INT32, s3 INT32)");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg15.d1(timestamp, s1_physical, s2, s3) ALIGNED VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      // Create alias series from aligned series
      statement.execute(
          "ALTER TIMESERIES root.sg15.d1.s1_physical RENAME TO root.sg15.d1.s1_alias");

      // Test: Delete timeseries using alias path (should delete from aligned series)
      statement.execute("DELETE TIMESERIES root.sg15.d1.s1_alias");

      // Verify alias path no longer exists
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg15.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify physical path no longer exists (s1_physical was part of aligned series)
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg15.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify other aligned measurements still exist
      try (ResultSet resultSet = statement.executeQuery("SELECT s2, s3 FROM root.sg15.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count); // Should still have all data
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with cross-database alias from aligned series. */
  @Test
  public void testDeleteDataWithCrossDatabaseAliasFromAlignedSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create aligned timeseries in different databases
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.db10.d1(s1_physical INT32, s2 INT32, s3 INT32)");
      statement.execute("CREATE TIMESERIES root.db11.d1.s4 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db10.d1(timestamp, s1_physical, s2, s3) ALIGNED VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
        statement.execute(
            String.format(
                "INSERT INTO root.db11.d1(timestamp, s4) VALUES(%d, %d)", i * 10, i * 10));
      }

      // Create cross-database alias from aligned series
      statement.execute(
          "ALTER TIMESERIES root.db10.d1.s1_physical RENAME TO root.db11.d1.s1_alias");

      // Verify alias exists
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.db11.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Test: Delete data from cross-database alias (should delete from aligned series)
      statement.execute("DELETE FROM root.db11.d1.s1_alias WHERE time <= 50");

      // Verify alias data was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.db11.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (60-100)
      }

      // Verify other aligned measurements in source database are not affected
      try (ResultSet resultSet = statement.executeQuery("SELECT s2, s3 FROM root.db10.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count); // All 10 rows should remain
      }

      // Test: Delete timeseries using cross-database alias
      statement.execute("DELETE TIMESERIES root.db11.d1.s1_alias");

      // Verify alias path no longer exists
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.db11.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify physical path in aligned series no longer exists
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db10.d1")) {
        assertFalse(resultSet.next());
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with multiple alias from same aligned series. */
  @Test
  public void testDeleteDataWithMultipleAliasFromAlignedSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create aligned timeseries
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.sg16.d1(s1_physical INT32, s2_physical INT32, s3 INT32, s4 INT32)");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg16.d1(timestamp, s1_physical, s2_physical, s3, s4) ALIGNED VALUES(%d, %d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3, i * 4));
      }

      // Create multiple alias series from same aligned series
      statement.execute(
          "ALTER TIMESERIES root.sg16.d1.s1_physical RENAME TO root.sg16.d1.s1_alias");
      statement.execute(
          "ALTER TIMESERIES root.sg16.d1.s2_physical RENAME TO root.sg16.d1.s2_alias");

      // Verify aliases exist
      try (ResultSet resultSet =
          statement.executeQuery("SELECT s1_alias, s2_alias FROM root.sg16.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Test: Delete data from multiple alias series
      statement.execute(
          "DELETE FROM root.sg16.d1.s1_alias, root.sg16.d1.s2_alias WHERE time <= 50");

      // Verify alias data was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg16.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (60-100)
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s2_alias FROM root.sg16.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (60-100)
      }

      // Verify other aligned measurements are not affected
      try (ResultSet resultSet = statement.executeQuery("SELECT s3, s4 FROM root.sg16.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count); // All 10 rows should remain
      }

      // Test: Delete timeseries using pattern that includes multiple aliases
      statement.execute("DELETE TIMESERIES root.sg16.d1.**");

      // Verify alias paths no longer exist
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg16.d1")) {
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s2_alias FROM root.sg16.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify physical paths no longer exist
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg16.d1")) {
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet = statement.executeQuery("SELECT s2_physical FROM root.sg16.d1")) {
        assertFalse(resultSet.next());
      }
    }
  }

  /** Test delete database with alias from aligned series. */
  @Test
  public void testDeleteDatabaseWithAliasFromAlignedSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create aligned timeseries in multiple databases
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.db12.d1(s1_physical INT32, s2 INT32, s3 INT32)");
      statement.execute("CREATE TIMESERIES root.db13.d1.s4 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db12.d1(timestamp, s1_physical, s2, s3) ALIGNED VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
        statement.execute(
            String.format(
                "INSERT INTO root.db13.d1(timestamp, s4) VALUES(%d, %d)", i * 10, i * 10));
      }

      // Create cross-database alias from aligned series
      statement.execute(
          "ALTER TIMESERIES root.db12.d1.s1_physical RENAME TO root.db13.d1.s1_alias");

      // Test: Try to delete database with aligned series that has alias
      try {
        statement.execute("DELETE DATABASE root.db12");
        fail("Delete database should fail");
      } catch (SQLException e) {
        // Expected: should throw exception if database contains aligned series with alias
        assertTrue(
            e.getMessage().contains("alias")
                || e.getMessage().contains("invalid")
                || e.getMessage().contains("renamed")
                || e.getMessage().contains("original path"));
      }

      // Test: Try to delete database that is target of alias from aligned series
      statement.execute("DELETE DATABASE root.db13");

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test multiple renames and restore original physical name. */
  @Test
  public void testMultipleRenamesAndRestore() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries with _physical suffix
      statement.execute(
          "CREATE TIMESERIES root.sg17.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg17.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert initial data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg17.d1(timestamp, s1_physical, s2) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      // Verify initial data
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg17.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // First rename: s1_physical -> s1_alias
      statement.execute(
          "ALTER TIMESERIES root.sg17.d1.s1_physical RENAME TO root.sg17.d1.s1_alias");

      // Verify first rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg17.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Delete some data after first rename
      statement.execute("DELETE FROM root.sg17.d1.s1_alias WHERE time <= 30");

      // Verify deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg17.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(7, count); // 7 rows should remain (40-100)
      }

      // Insert more data after first rename
      for (int i = 11; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg17.d1(timestamp, s1_alias, s2) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      // Second rename: s1_alias -> s1_alias
      statement.execute("ALTER TIMESERIES root.sg17.d1.s1_alias RENAME TO root.sg17.d1.s1_alias");

      // Verify second rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg17.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(12, count); // 7 + 5 = 12 rows
      }

      // Delete some data after second rename
      statement.execute("DELETE FROM root.sg17.d1.s1_alias WHERE time <= 50");

      // Verify deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg17.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count); // 10 rows should remain (60-150)
      }

      // Third rename: s1_alias -> s1_alias2
      statement.execute("ALTER TIMESERIES root.sg17.d1.s1_alias RENAME TO root.sg17.d1.s1_alias2");

      // Verify third rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias2 FROM root.sg17.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Restore original physical name: s1_alias2 -> s1_physical
      statement.execute(
          "ALTER TIMESERIES root.sg17.d1.s1_alias2 RENAME TO root.sg17.d1.s1_physical");

      // Verify restore
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg17.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Test delete after restore
      statement.execute("DELETE FROM root.sg17.d1.s1_physical WHERE time <= 100");

      // Verify deletion after restore
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg17.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (110-150)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test rename across different databases multiple times. */
  @Test
  public void testRenameAcrossDatabasesMultipleTimes() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries in different databases
      statement.execute(
          "CREATE TIMESERIES root.db14.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db15.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db16.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db14.d1(timestamp, s1_physical) VALUES(%d, %d)", i * 10, i));
        statement.execute(
            String.format("INSERT INTO root.db15.d1(timestamp, s2) VALUES(%d, %d)", i * 10, i * 2));
        statement.execute(
            String.format("INSERT INTO root.db16.d1(timestamp, s3) VALUES(%d, %d)", i * 10, i * 3));
      }

      // First rename: db14 -> db15
      statement.execute(
          "ALTER TIMESERIES root.db14.d1.s1_physical RENAME TO root.db15.d1.s1_alias");

      // Verify first rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.db15.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Delete some data
      statement.execute("DELETE FROM root.db15.d1.s1_alias WHERE time <= 40");

      // Second rename: db15 -> db16
      statement.execute("ALTER TIMESERIES root.db15.d1.s1_alias RENAME TO root.db16.d1.s1_alias2");

      // Verify second rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias2 FROM root.db16.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(6, count); // 6 rows should remain (50-100)
      }

      // Insert more data
      for (int i = 11; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db16.d1(timestamp, s1_alias2, s3) VALUES(%d, %d, %d)",
                i * 10, i, i * 3));
      }

      // Third rename: db16 -> db14 (back to original database)
      statement.execute(
          "ALTER TIMESERIES root.db16.d1.s1_alias2 RENAME TO root.db14.d1.s1_physical");

      // Verify third rename (restore to original database and name)
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(11, count); // 6 + 5 = 11 rows
      }

      // Test delete after restore
      statement.execute("DELETE FROM root.db14.d1.s1_physical WHERE time <= 100");

      // Verify deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db14.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (110-150)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test rename across different devices multiple times. */
  @Test
  public void testRenameAcrossDevicesMultipleTimes() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries in different devices
      statement.execute(
          "CREATE TIMESERIES root.sg18.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg18.d2.s2 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg18.d3.s3 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg18.d1(timestamp, s1_physical) VALUES(%d, %d)", i * 10, i));
        statement.execute(
            String.format("INSERT INTO root.sg18.d2(timestamp, s2) VALUES(%d, %d)", i * 10, i * 2));
        statement.execute(
            String.format("INSERT INTO root.sg18.d3(timestamp, s3) VALUES(%d, %d)", i * 10, i * 3));
      }

      // First rename: d1 -> d2
      statement.execute(
          "ALTER TIMESERIES root.sg18.d1.s1_physical RENAME TO root.sg18.d2.s1_alias");

      // Verify first rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg18.d2")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Delete some data
      statement.execute("DELETE FROM root.sg18.d2.s1_alias WHERE time <= 30");

      // Second rename: d2 -> d3
      statement.execute("ALTER TIMESERIES root.sg18.d2.s1_alias RENAME TO root.sg18.d3.s1_alias2");

      // Verify second rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias2 FROM root.sg18.d3")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(7, count); // 7 rows should remain (40-100)
      }

      // Insert more data
      for (int i = 11; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg18.d3(timestamp, s1_alias2, s3) VALUES(%d, %d, %d)",
                i * 10, i, i * 3));
      }

      // Third rename: d3 -> d1 (back to original device)
      statement.execute(
          "ALTER TIMESERIES root.sg18.d3.s1_alias2 RENAME TO root.sg18.d1.s1_physical");

      // Verify third rename (restore to original device and name)
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg18.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(12, count); // 7 + 5 = 12 rows
      }

      // Test delete after restore
      statement.execute("DELETE FROM root.sg18.d1.s1_physical WHERE time <= 100");

      // Verify deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg18.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (110-150)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test rename aligned series multiple times. */
  @Test
  public void testRenameAlignedSeriesMultipleTimes() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create aligned timeseries
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.sg19.d1(s1_physical INT32, s2 INT32, s3 INT32)");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg19.d1(timestamp, s1_physical, s2, s3) ALIGNED VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      // First rename: s1_physical -> s1_alias
      statement.execute(
          "ALTER TIMESERIES root.sg19.d1.s1_physical RENAME TO root.sg19.d1.s1_alias");

      // Verify first rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg19.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Delete some data
      statement.execute("DELETE FROM root.sg19.d1.s1_alias WHERE time <= 40");

      // Second rename: s1_alias -> s1_alias
      statement.execute("ALTER TIMESERIES root.sg19.d1.s1_alias RENAME TO root.sg19.d1.s1_alias");

      // Verify second rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg19.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(6, count); // 6 rows should remain (50-100)
      }

      // Insert more data
      for (int i = 11; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg19.d1(timestamp, s1_alias, s2, s3) ALIGNED VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      // Third rename: s1_alias -> s1_physical (restore original name)
      statement.execute(
          "ALTER TIMESERIES root.sg19.d1.s1_alias RENAME TO root.sg19.d1.s1_physical");

      // Verify third rename (restore)
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg19.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(11, count); // 6 + 5 = 11 rows
      }

      // Verify other aligned measurements are not affected
      try (ResultSet resultSet = statement.executeQuery("SELECT s2, s3 FROM root.sg19.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(15, count); // All 15 rows should remain
      }

      // Test delete after restore
      statement.execute("DELETE FROM root.sg19.d1.s1_physical WHERE time <= 100");

      // Verify deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg19.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (110-150)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test rename across database and device multiple times. */
  @Test
  public void testRenameAcrossDatabaseAndDeviceMultipleTimes() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries in different databases and devices
      statement.execute(
          "CREATE TIMESERIES root.db17.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db18.d2.s2 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db19.d3.s3 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db17.d1(timestamp, s1_physical) VALUES(%d, %d)", i * 10, i));
        statement.execute(
            String.format("INSERT INTO root.db18.d2(timestamp, s2) VALUES(%d, %d)", i * 10, i * 2));
        statement.execute(
            String.format("INSERT INTO root.db19.d3(timestamp, s3) VALUES(%d, %d)", i * 10, i * 3));
      }

      // First rename: db17.d1 -> db18.d2
      statement.execute(
          "ALTER TIMESERIES root.db17.d1.s1_physical RENAME TO root.db18.d2.s1_alias");

      // Verify first rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.db18.d2")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count);
      }

      // Delete some data
      statement.execute("DELETE FROM root.db18.d2.s1_alias WHERE time <= 30");

      // Second rename: db18.d2 -> db19.d3
      statement.execute("ALTER TIMESERIES root.db18.d2.s1_alias RENAME TO root.db19.d3.s1_alias2");

      // Verify second rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias2 FROM root.db19.d3")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(7, count); // 7 rows should remain (40-100)
      }

      // Insert more data
      for (int i = 11; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db19.d3(timestamp, s1_alias2, s3) VALUES(%d, %d, %d)",
                i * 10, i, i * 3));
      }

      // Third rename: db19.d3 -> db17.d1 (back to original database and device)
      statement.execute(
          "ALTER TIMESERIES root.db19.d3.s1_alias2 RENAME TO root.db17.d1.s1_physical");

      // Verify third rename (restore)
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db17.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(12, count); // 7 + 5 = 12 rows
      }

      // Test delete after restore
      statement.execute("DELETE FROM root.db17.d1.s1_physical WHERE time <= 100");

      // Verify deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db17.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (110-150)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with multiple renames and operations. */
  @Test
  public void testDeleteDataWithMultipleRenamesAndOperations() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute(
          "CREATE TIMESERIES root.sg20.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg20.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert initial data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg20.d1(timestamp, s1_physical, s2) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      // First rename
      statement.execute(
          "ALTER TIMESERIES root.sg20.d1.s1_physical RENAME TO root.sg20.d1.s1_alias");

      // Delete data after first rename
      statement.execute("DELETE FROM root.sg20.d1.s1_alias WHERE time <= 30");

      // Insert data after first rename
      for (int i = 11; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg20.d1(timestamp, s1_alias, s2) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      // Second rename
      statement.execute("ALTER TIMESERIES root.sg20.d1.s1_alias RENAME TO root.sg20.d1.s1_alias");

      // Delete data after second rename
      statement.execute("DELETE FROM root.sg20.d1.s1_alias WHERE time <= 50");

      // Insert data after second rename
      for (int i = 16; i <= 20; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg20.d1(timestamp, s1_alias, s2) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      // Verify current state
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg20.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(15, count); // 7 + 5 + 3 = 15 rows (60-200)
      }

      // Delete using pattern
      statement.execute("DELETE FROM root.sg20.d1.s* WHERE time <= 150");

      // Verify deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg20.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (160-200)
      }

      // Restore original name
      statement.execute(
          "ALTER TIMESERIES root.sg20.d1.s1_alias RENAME TO root.sg20.d1.s1_physical");

      // Verify restore
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg20.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count);
      }

      // Final delete
      statement.execute("DELETE FROM root.sg20.d1.s1_physical WHERE time <= 200");

      // Verify final deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg20.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete timeseries with multiple renames. */
  @Test
  public void testDeleteTimeSeriesWithMultipleRenames() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute(
          "CREATE TIMESERIES root.sg21.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg21.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg21.d1(timestamp, s1_physical, s2) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      // First rename
      statement.execute(
          "ALTER TIMESERIES root.sg21.d1.s1_physical RENAME TO root.sg21.d1.s1_alias");

      // Second rename
      statement.execute("ALTER TIMESERIES root.sg21.d1.s1_alias RENAME TO root.sg21.d1.s1_alias");

      // Third rename
      statement.execute("ALTER TIMESERIES root.sg21.d1.s1_alias RENAME TO root.sg21.d1.s1_alias2");

      // Test: Delete timeseries using latest alias
      statement.execute("DELETE TIMESERIES root.sg21.d1.s1_alias2");

      // Verify deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias2 FROM root.sg21.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify physical path no longer exists
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg21.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify all previous aliases no longer exist
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg21.d1")) {
        assertFalse(resultSet.next());
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with aligned series multiple renames. */
  @Test
  public void testDeleteDataWithAlignedSeriesMultipleRenames() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create aligned timeseries
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.sg22.d1(s1_physical INT32, s2 INT32, s3 INT32)");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg22.d1(timestamp, s1_physical, s2, s3) ALIGNED VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      // First rename: s1_physical -> s1_alias
      statement.execute(
          "ALTER TIMESERIES root.sg22.d1.s1_physical RENAME TO root.sg22.d1.s1_alias");

      // Delete data after first rename
      statement.execute("DELETE FROM root.sg22.d1.s1_alias WHERE time <= 30");

      // Insert data after first rename
      for (int i = 11; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg22.d1(timestamp, s1_alias, s2, s3) ALIGNED VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      // Second rename: s1_alias -> s1_alias
      statement.execute("ALTER TIMESERIES root.sg22.d1.s1_alias RENAME TO root.sg22.d1.s1_alias");

      // Delete data after second rename
      statement.execute("DELETE FROM root.sg22.d1.s1_alias WHERE time <= 50");

      // Insert data after second rename
      for (int i = 16; i <= 20; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg22.d1(timestamp, s1_alias, s2, s3) ALIGNED VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      // Verify current state
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg22.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(15, count); // 7 + 5 + 3 = 15 rows (60-200)
      }

      // Verify other aligned measurements
      try (ResultSet resultSet = statement.executeQuery("SELECT s2, s3 FROM root.sg22.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(20, count); // All 20 rows should remain
      }

      // Delete using pattern
      statement.execute("DELETE FROM root.sg22.d1.s* WHERE time <= 150");

      // Verify deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg22.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (160-200)
      }

      // Restore original name
      statement.execute(
          "ALTER TIMESERIES root.sg22.d1.s1_alias RENAME TO root.sg22.d1.s1_physical");

      // Verify restore
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg22.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count);
      }

      // Final delete
      statement.execute("DELETE FROM root.sg22.d1.s1_physical WHERE time <= 200");

      // Verify final deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg22.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete timeseries with aligned series multiple renames. */
  @Test
  public void testDeleteTimeSeriesWithAlignedSeriesMultipleRenames() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create aligned timeseries
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.sg23.d1(s1_physical INT32, s2 INT32, s3 INT32)");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg23.d1(timestamp, s1_physical, s2, s3) ALIGNED VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      // First rename
      statement.execute(
          "ALTER TIMESERIES root.sg23.d1.s1_physical RENAME TO root.sg23.d1.s1_alias");

      // Second rename
      statement.execute("ALTER TIMESERIES root.sg23.d1.s1_alias RENAME TO root.sg23.d1.s1_alias1");

      // Third rename
      statement.execute("ALTER TIMESERIES root.sg23.d1.s1_alias1 RENAME TO root.sg23.d1.s1_alias2");

      // Verify physical path no longer exists
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg23.d1")) {
        assertFalse(resultSet.next());
      }

      // Test: Delete timeseries using latest alias
      statement.execute("DELETE TIMESERIES root.sg23.d1.s1_alias2");

      // Verify deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias2 FROM root.sg23.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify other aligned measurements still exist
      try (ResultSet resultSet = statement.executeQuery("SELECT s2, s3 FROM root.sg23.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count); // Should still have all data
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test complex scenario: multiple renames, deletes, inserts across databases and devices. */
  @Test
  public void testComplexScenarioMultipleRenamesDeletesInserts() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries in multiple databases and devices
      statement.execute(
          "CREATE TIMESERIES root.db20.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db20.d2.s2 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db21.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db21.d2.s4 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert initial data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db20.d1(timestamp, s1_physical) VALUES(%d, %d)", i * 10, i));
        statement.execute(
            String.format("INSERT INTO root.db20.d2(timestamp, s2) VALUES(%d, %d)", i * 10, i * 2));
        statement.execute(
            String.format("INSERT INTO root.db21.d1(timestamp, s3) VALUES(%d, %d)", i * 10, i * 3));
        statement.execute(
            String.format("INSERT INTO root.db21.d2(timestamp, s4) VALUES(%d, %d)", i * 10, i * 4));
      }

      // First rename: db20.d1 -> db20.d2
      statement.execute(
          "ALTER TIMESERIES root.db20.d1.s1_physical RENAME TO root.db20.d2.s1_alias");

      // Delete data after first rename
      statement.execute("DELETE FROM root.db20.d2.s1_alias WHERE time <= 30");

      // Insert data after first rename
      for (int i = 11; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db20.d2(timestamp, s1_alias, s2) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      // Second rename: db20.d2 -> db21.d1
      statement.execute("ALTER TIMESERIES root.db20.d2.s1_alias RENAME TO root.db21.d1.s1_alias2");

      // Delete data after second rename
      statement.execute("DELETE FROM root.db21.d1.s1_alias2 WHERE time <= 50");

      // Insert data after second rename
      for (int i = 16; i <= 20; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db21.d1(timestamp, s1_alias2, s3) VALUES(%d, %d, %d)",
                i * 10, i, i * 3));
      }

      // Third rename: db21.d1 -> db21.d2
      statement.execute("ALTER TIMESERIES root.db21.d1.s1_alias2 RENAME TO root.db21.d2.s1_alias3");

      // Delete data after third rename
      statement.execute("DELETE FROM root.db21.d2.s1_alias3 WHERE time <= 100");

      // Insert data after third rename
      for (int i = 21; i <= 25; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db21.d2(timestamp, s1_alias3, s4) VALUES(%d, %d, %d)",
                i * 10, i, i * 4));
      }

      // Verify current state
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias3 FROM root.db21.d2")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(15, count); // 7 + 5 + 3 = 15 rows (110-250)
      }

      // Restore to original database and device: db21.d2 -> db20.d1
      statement.execute(
          "ALTER TIMESERIES root.db21.d2.s1_alias3 RENAME TO root.db20.d1.s1_physical");

      // Verify restore
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db20.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(15, count);
      }

      // Final delete
      statement.execute("DELETE FROM root.db20.d1.s1_physical WHERE time <= 200");

      // Verify final deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db20.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (210-250)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete with pattern matching after multiple renames. */
  @Test
  public void testDeleteWithPatternAfterMultipleRenames() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute(
          "CREATE TIMESERIES root.sg24.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg24.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg24.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg24.d1(timestamp, s1_physical, s2_physical, s3) VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      // Rename s1_physical multiple times
      statement.execute(
          "ALTER TIMESERIES root.sg24.d1.s1_physical RENAME TO root.sg24.d1.s1_alias");
      statement.execute("ALTER TIMESERIES root.sg24.d1.s1_alias RENAME TO root.sg24.d1.s1_alias");
      statement.execute("ALTER TIMESERIES root.sg24.d1.s1_alias RENAME TO root.sg24.d1.s1_alias2");

      // Rename s2_physical
      statement.execute(
          "ALTER TIMESERIES root.sg24.d1.s2_physical RENAME TO root.sg24.d1.s2_alias");

      // Test: Delete using pattern s*_alias* (should match s1_alias2 and s2_alias)
      statement.execute(
          "DELETE FROM root.sg24.d1.s1_alias2, root.sg24.d1.s2_alias WHERE time <= 50");

      // Verify s1_alias2 was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias2 FROM root.sg24.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (60-100)
      }

      // Verify s2_alias was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s2_alias FROM root.sg24.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (60-100)
      }

      // Verify s3 was NOT deleted (doesn't match pattern)
      try (ResultSet resultSet = statement.executeQuery("SELECT s3 FROM root.sg24.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count); // All 10 rows should remain
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete timeseries with pattern after multiple renames. */
  @Test
  public void testDeleteTimeSeriesWithPatternAfterMultipleRenames() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute(
          "CREATE TIMESERIES root.sg25.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg25.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg25.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg25.d1.t1 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg25.d1(timestamp, s1_physical, s2_physical, s3, t1) VALUES(%d, %d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3, i * 4));
      }

      // Rename s1_physical multiple times
      statement.execute(
          "ALTER TIMESERIES root.sg25.d1.s1_physical RENAME TO root.sg25.d1.s1_alias");
      statement.execute("ALTER TIMESERIES root.sg25.d1.s1_alias RENAME TO root.sg25.d1.s1_alias");
      statement.execute("ALTER TIMESERIES root.sg25.d1.s1_alias RENAME TO root.sg25.d1.s1_alias2");

      // Rename s2_physical
      statement.execute(
          "ALTER TIMESERIES root.sg25.d1.s2_physical RENAME TO root.sg25.d1.s2_alias");

      // Test: Delete timeseries s1_alias2 and s2_alias
      statement.execute("DELETE TIMESERIES root.sg25.d1.s1_alias2");
      statement.execute("DELETE TIMESERIES root.sg25.d1.s2_alias");

      // Verify s1_alias2 was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias2 FROM root.sg25.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify s2_alias was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s2_alias FROM root.sg25.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify s3_physical was NOT deleted (doesn't match pattern)
      try (ResultSet resultSet = statement.executeQuery("SELECT s3_physical FROM root.sg25.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }

      // Verify t1 was NOT deleted (doesn't match pattern)
      try (ResultSet resultSet = statement.executeQuery("SELECT t1 FROM root.sg25.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count); // All 10 rows should remain
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete with multiple renames and restore in same database and device. */
  @Test
  public void testDeleteWithMultipleRenamesAndRestoreSameLocation() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute(
          "CREATE TIMESERIES root.sg26.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 20; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg26.d1(timestamp, s1_physical) VALUES(%d, %d)", i * 10, i));
      }

      // First rename: s1_physical -> s1_alias
      statement.execute(
          "ALTER TIMESERIES root.sg26.d1.s1_physical RENAME TO root.sg26.d1.s1_alias");

      // Query after first rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.sg26.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(20, count);
      }

      // Delete data
      statement.execute("DELETE FROM root.sg26.d1.s1_alias WHERE time <= 50");

      // Insert more data
      for (int i = 21; i <= 25; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg26.d1(timestamp, s1_alias) VALUES(%d, %d)", i * 10, i));
      }

      // Second rename: s1_alias -> s1_alias2
      statement.execute("ALTER TIMESERIES root.sg26.d1.s1_alias RENAME TO root.sg26.d1.s1_alias2");

      // Query after second rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias2 FROM root.sg26.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(20, count); // 15 + 5 = 20 rows
      }

      // Delete data
      statement.execute("DELETE FROM root.sg26.d1.s1_alias2 WHERE time <= 100");

      // Insert more data
      for (int i = 26; i <= 30; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg26.d1(timestamp, s1_alias2) VALUES(%d, %d)", i * 10, i));
      }

      // Third rename: s1_alias2 -> s1_alias3
      statement.execute("ALTER TIMESERIES root.sg26.d1.s1_alias2 RENAME TO root.sg26.d1.s1_alias3");

      // Query after third rename
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias3 FROM root.sg26.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(20, count); // 10 + 5 + 5 = 20 rows
      }

      // Restore original name: s1_alias3 -> s1_physical
      statement.execute(
          "ALTER TIMESERIES root.sg26.d1.s1_alias3 RENAME TO root.sg26.d1.s1_physical");

      // Query after restore
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg26.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(20, count);
      }

      // Final delete
      statement.execute("DELETE FROM root.sg26.d1.s1_physical WHERE time <= 200");

      // Verify final state
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg26.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count); // 10 rows should remain (210-300)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete with rename chain: physical -> original -> alias -> original2 -> physical. */
  @Test
  public void testDeleteWithRenameChain() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute(
          "CREATE TIMESERIES root.sg27.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg27.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg27.d1(timestamp, s1_physical, s2_physical) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      // Rename chain for s1: physical -> original -> alias -> original2 -> physical
      statement.execute(
          "ALTER TIMESERIES root.sg27.d1.s1_physical RENAME TO root.sg27.d1.s1_alias");
      statement.execute("ALTER TIMESERIES root.sg27.d1.s1_alias RENAME TO root.sg27.d1.s1_alias");
      statement.execute("ALTER TIMESERIES root.sg27.d1.s1_alias RENAME TO root.sg27.d1.s1_alias2");
      statement.execute(
          "ALTER TIMESERIES root.sg27.d1.s1_alias2 RENAME TO root.sg27.d1.s1_physical");

      // Verify restore
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg27.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(15, count);
      }

      // Delete data after restore
      statement.execute("DELETE FROM root.sg27.d1.s1_physical WHERE time <= 80");

      // Verify deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg27.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(7, count); // 7 rows should remain (90-150)
      }

      // Verify s2_physical is not affected
      try (ResultSet resultSet = statement.executeQuery("SELECT s2_physical FROM root.sg27.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(15, count); // All 15 rows should remain
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete with rename across multiple databases and devices with operations. */
  @Test
  public void testDeleteWithRenameAcrossMultipleDatabasesAndDevices() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries in multiple databases and devices
      statement.execute(
          "CREATE TIMESERIES root.db22.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.db22.d2.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.db23.d1.s3_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.db23.d2.s4_physical WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db22.d1(timestamp, s1_physical) VALUES(%d, %d)", i * 10, i));
        statement.execute(
            String.format(
                "INSERT INTO root.db22.d2(timestamp, s2_physical) VALUES(%d, %d)", i * 10, i * 2));
        statement.execute(
            String.format(
                "INSERT INTO root.db23.d1(timestamp, s3_physical) VALUES(%d, %d)", i * 10, i * 3));
        statement.execute(
            String.format(
                "INSERT INTO root.db23.d2(timestamp, s4_physical) VALUES(%d, %d)", i * 10, i * 4));
      }

      // Rename chain: db22.d1 -> db22.d2 -> db23.d1 -> db23.d2 -> db22.d1
      statement.execute(
          "ALTER TIMESERIES root.db22.d1.s1_physical RENAME TO root.db22.d2.s1_alias");

      // Delete and insert after first rename
      statement.execute("DELETE FROM root.db22.d2.s1_alias WHERE time <= 30");
      for (int i = 11; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db22.d2(timestamp, s1_alias, s2_physical) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      // Verify data count after first rename: 7 (40-100) + 5 (110-150) = 12
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias FROM root.db22.d2")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(12, count);
      }

      statement.execute("ALTER TIMESERIES root.db22.d2.s1_alias RENAME TO root.db23.d1.s1_alias2");

      // Delete and insert after second rename
      statement.execute("DELETE FROM root.db23.d1.s1_alias2 WHERE time <= 50");
      for (int i = 16; i <= 20; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db23.d1(timestamp, s1_alias2, s3_physical) VALUES(%d, %d, %d)",
                i * 10, i, i * 3));
      }

      // Verify data count after second rename: 10 (60-100, 110-150) + 5 (160-200) = 15
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias2 FROM root.db23.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(15, count);
      }

      statement.execute("ALTER TIMESERIES root.db23.d1.s1_alias2 RENAME TO root.db23.d2.s1_alias3");

      // Delete and insert after third rename
      statement.execute("DELETE FROM root.db23.d2.s1_alias3 WHERE time <= 100");
      for (int i = 21; i <= 25; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db23.d2(timestamp, s1_alias3, s4_physical) VALUES(%d, %d, %d)",
                i * 10, i, i * 4));
      }

      // Verify data count after third rename: 10 (110-150, 160-200) + 5 (210-250) = 15
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias3 FROM root.db23.d2")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(15, count);
      }

      // Restore to original location: db23.d2 -> db22.d1
      statement.execute(
          "ALTER TIMESERIES root.db23.d2.s1_alias3 RENAME TO root.db22.d1.s1_physical");

      // Verify restore
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db22.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(15, count); // 10 (110-150, 160-200) + 5 (210-250) = 15 rows
      }

      // Final delete
      statement.execute("DELETE FROM root.db22.d1.s1_physical WHERE time <= 200");

      // Verify final deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db22.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (210-250)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete with aligned series rename chain and operations. */
  @Test
  public void testDeleteWithAlignedSeriesRenameChain() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create aligned timeseries
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.sg28.d1(s1_physical INT32, s2 INT32, s3 INT32)");

      // Insert data
      for (int i = 1; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg28.d1(timestamp, s1_physical, s2, s3) ALIGNED VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      // Rename chain for s1: physical -> original -> alias -> original2 -> physical
      statement.execute(
          "ALTER TIMESERIES root.sg28.d1.s1_physical RENAME TO root.sg28.d1.s1_alias");

      // Delete and insert after first rename
      statement.execute("DELETE FROM root.sg28.d1.s1_alias WHERE time <= 40");
      for (int i = 16; i <= 20; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg28.d1(timestamp, s1_alias, s2, s3) ALIGNED VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      statement.execute("ALTER TIMESERIES root.sg28.d1.s1_alias RENAME TO root.sg28.d1.s1_alias");

      // Delete and insert after second rename
      statement.execute("DELETE FROM root.sg28.d1.s1_alias WHERE time <= 60");
      for (int i = 21; i <= 25; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg28.d1(timestamp, s1_alias, s2, s3) ALIGNED VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      statement.execute("ALTER TIMESERIES root.sg28.d1.s1_alias RENAME TO root.sg28.d1.s1_alias2");

      // Delete and insert after third rename
      statement.execute("DELETE FROM root.sg28.d1.s1_alias2 WHERE time <= 100");
      for (int i = 26; i <= 30; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg28.d1(timestamp, s1_alias2, s2, s3) ALIGNED VALUES(%d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3));
      }

      // Restore original name
      statement.execute(
          "ALTER TIMESERIES root.sg28.d1.s1_alias2 RENAME TO root.sg28.d1.s1_physical");

      // Verify restore
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg28.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(20, count); // 15 (110-150, 160-200, 210-250) + 5 (260-300) = 20 rows
      }

      // Final delete
      statement.execute("DELETE FROM root.sg28.d1.s1_physical WHERE time <= 200");

      // Verify final deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg28.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count); // 10 rows should remain (210-300)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete timeseries with rename chain and pattern matching. */
  @Test
  public void testDeleteTimeSeriesWithRenameChainAndPattern() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute(
          "CREATE TIMESERIES root.sg29.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg29.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg29.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.sg29.d1.t1 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg29.d1(timestamp, s1_physical, s2_physical, s3, t1) VALUES(%d, %d, %d, %d, %d)",
                i * 10, i, i * 2, i * 3, i * 4));
      }

      // Rename s1_physical multiple times
      statement.execute(
          "ALTER TIMESERIES root.sg29.d1.s1_physical RENAME TO root.sg29.d1.s1_alias");
      statement.execute("ALTER TIMESERIES root.sg29.d1.s1_alias RENAME TO root.sg29.d1.s1_alias");
      statement.execute("ALTER TIMESERIES root.sg29.d1.s1_alias RENAME TO root.sg29.d1.s1_alias2");

      // Rename s2_physical
      statement.execute(
          "ALTER TIMESERIES root.sg29.d1.s2_physical RENAME TO root.sg29.d1.s2_alias");

      // Test: Delete timeseries s1_alias2 and s2_alias
      statement.execute("DELETE TIMESERIES root.sg29.d1.s1_alias2");
      statement.execute("DELETE TIMESERIES root.sg29.d1.s2_alias");

      // Verify s1_alias2 was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias2 FROM root.sg29.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify s2_alias was deleted
      try (ResultSet resultSet = statement.executeQuery("SELECT s2_alias FROM root.sg29.d1")) {
        assertFalse(resultSet.next());
      }

      // Verify s3_physical was NOT deleted (doesn't match pattern)
      try (ResultSet resultSet = statement.executeQuery("SELECT s3_physical FROM root.sg29.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }

      // Verify t1 was NOT deleted (doesn't match pattern)
      try (ResultSet resultSet = statement.executeQuery("SELECT t1 FROM root.sg29.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(10, count); // All 10 rows should remain
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete data with rename, restore, and multiple operations. */
  @Test
  public void testDeleteDataWithRenameRestoreAndOperations() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries
      statement.execute(
          "CREATE TIMESERIES root.sg30.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.sg30.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert initial data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg30.d1(timestamp, s1_physical, s2_physical) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      // Rename: physical -> original
      statement.execute(
          "ALTER TIMESERIES root.sg30.d1.s1_physical RENAME TO root.sg30.d1.s1_alias");

      // Delete data
      statement.execute("DELETE FROM root.sg30.d1.s1_alias WHERE time <= 30");

      // Insert data
      for (int i = 11; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg30.d1(timestamp, s1_alias, s2_physical) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      // Rename: original -> alias
      statement.execute("ALTER TIMESERIES root.sg30.d1.s1_alias RENAME TO root.sg30.d1.s1_alias");

      // Delete data
      statement.execute("DELETE FROM root.sg30.d1.s1_alias WHERE time <= 50");

      // Insert data
      for (int i = 16; i <= 20; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.sg30.d1(timestamp, s1_alias, s2_physical) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      // Restore: alias -> physical
      statement.execute(
          "ALTER TIMESERIES root.sg30.d1.s1_alias RENAME TO root.sg30.d1.s1_physical");

      // Verify restore
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg30.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(15, count); // 7 + 5 + 3 = 15 rows
      }

      // Delete using pattern
      statement.execute("DELETE FROM root.sg30.d1.s* WHERE time <= 150");

      // Verify deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.sg30.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (160-200)
      }

      // Verify s2_physical was also deleted (matches pattern s*)
      try (ResultSet resultSet = statement.executeQuery("SELECT s2_physical FROM root.sg30.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (160-200)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete with cross-database rename chain and restore. */
  @Test
  public void testDeleteWithCrossDatabaseRenameChainAndRestore() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create timeseries in different databases
      statement.execute(
          "CREATE TIMESERIES root.db24.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db25.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db26.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db24.d1(timestamp, s1_physical) VALUES(%d, %d)", i * 10, i));
        statement.execute(
            String.format("INSERT INTO root.db25.d1(timestamp, s2) VALUES(%d, %d)", i * 10, i * 2));
        statement.execute(
            String.format("INSERT INTO root.db26.d1(timestamp, s3) VALUES(%d, %d)", i * 10, i * 3));
      }

      // Rename chain: db24 -> db25 -> db26 -> db24
      statement.execute(
          "ALTER TIMESERIES root.db24.d1.s1_physical RENAME TO root.db25.d1.s1_alias");

      // Delete and insert
      statement.execute("DELETE FROM root.db25.d1.s1_alias WHERE time <= 30");
      for (int i = 11; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db25.d1(timestamp, s1_alias, s2) VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
      }

      statement.execute("ALTER TIMESERIES root.db25.d1.s1_alias RENAME TO root.db26.d1.s1_alias2");

      // Delete and insert
      statement.execute("DELETE FROM root.db26.d1.s1_alias2 WHERE time <= 50");
      for (int i = 16; i <= 20; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db26.d1(timestamp, s1_alias2, s3) VALUES(%d, %d, %d)",
                i * 10, i, i * 3));
      }

      // Restore to original database
      statement.execute(
          "ALTER TIMESERIES root.db26.d1.s1_alias2 RENAME TO root.db24.d1.s1_physical");

      // Verify restore
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db24.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(15, count); // 7 + 5 + 3 = 15 rows
      }

      // Final delete
      statement.execute("DELETE FROM root.db24.d1.s1_physical WHERE time <= 150");

      // Verify final deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db24.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (160-200)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /** Test delete with aligned series cross-database rename chain. */
  @Test
  public void testDeleteWithAlignedSeriesCrossDatabaseRenameChain() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create aligned timeseries in different databases
      statement.execute("CREATE ALIGNED TIMESERIES root.db27.d1(s1_physical INT32, s2 INT32)");
      statement.execute("CREATE TIMESERIES root.db28.d1.s3 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db27.d1(timestamp, s1_physical, s2) ALIGNED VALUES(%d, %d, %d)",
                i * 10, i, i * 2));
        statement.execute(
            String.format("INSERT INTO root.db28.d1(timestamp, s3) VALUES(%d, %d)", i * 10, i * 3));
      }

      // Rename aligned series measurement across databases
      statement.execute(
          "ALTER TIMESERIES root.db27.d1.s1_physical RENAME TO root.db28.d1.s1_alias");

      // Delete and insert
      statement.execute("DELETE FROM root.db28.d1.s1_alias WHERE time <= 30");
      for (int i = 11; i <= 15; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db28.d1(timestamp, s1_alias, s3) VALUES(%d, %d, %d)",
                i * 10, i, i * 3));
      }

      // Rename again
      statement.execute("ALTER TIMESERIES root.db28.d1.s1_alias RENAME TO root.db28.d1.s1_alias2");

      // Verify restore
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_alias2 FROM root.db28.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(12, count);
      }

      // Delete and insert
      statement.execute("DELETE FROM root.db28.d1.s1_alias2 WHERE time <= 50");
      for (int i = 16; i <= 20; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db28.d1(timestamp, s1_alias2, s3) VALUES(%d, %d, %d)",
                i * 10, i, i * 3));
      }

      // Restore to original database
      statement.execute(
          "ALTER TIMESERIES root.db28.d1.s1_alias2 RENAME TO root.db27.d1.s1_physical");

      // Verify restore
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db27.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(15, count); // 7 + 5 + 3 = 15 rows
      }

      // Verify other aligned measurement is not affected
      try (ResultSet resultSet = statement.executeQuery("SELECT s2_physical FROM root.db27.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count);
      }

      // Final delete
      statement.execute("DELETE FROM root.db27.d1.s1_physical WHERE time <= 150");

      // Verify final deletion
      try (ResultSet resultSet = statement.executeQuery("SELECT s1_physical FROM root.db27.d1")) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(5, count); // 5 rows should remain (160-200)
      }

      // Clean up
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }

  /**
   * Test delete database with alias series: delete physical database should fail, then delete alias
   * database should succeed.
   */
  @Test
  public void testDeleteDatabaseWithPhysicalSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Setup: Create 4 databases - 1 for alias series, 3 for physical series
      statement.execute(
          "CREATE TIMESERIES root.db29.d1.s1_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.db30.d1.s2_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.db31.d1.s3_physical WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.db32.d1.s4 WITH DATATYPE=INT32, ENCODING=RLE");

      // Insert data
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "INSERT INTO root.db29.d1(timestamp, s1_physical) VALUES(%d, %d)", i * 10, i));
        statement.execute(
            String.format(
                "INSERT INTO root.db30.d1(timestamp, s2_physical) VALUES(%d, %d)", i * 10, i * 2));
        statement.execute(
            String.format(
                "INSERT INTO root.db31.d1(timestamp, s3_physical) VALUES(%d, %d)", i * 10, i * 3));
        statement.execute(
            String.format("INSERT INTO root.db32.d1(timestamp, s4) VALUES(%d, %d)", i * 10, i * 4));
      }

      // Rename physical series to create alias in db32 (alias database)
      statement.execute(
          "ALTER TIMESERIES root.db29.d1.s1_physical RENAME TO root.db32.d1.s1_alias");

      // Test: Try to delete physical series database (db29) - should fail
      try {
        statement.execute("DELETE DATABASE root.db29");
        fail(
            "Delete database root.db29 should fail because it contains physical series with alias");
      } catch (SQLException e) {
        // Expected: should throw exception if database contains physical series with alias
        assertTrue(
            e.getMessage()
                .contains(
                    "TSStatus(code:507, message:cannot delete database root.db29: contains 1 invalid time series. sample paths: [root.db29.d1.s1_physical])"));
      }

      // Verify db29 still exists
      try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        boolean db29Exists = false;
        while (resultSet.next()) {
          if (resultSet.getString(1).equals("root.db29")) {
            db29Exists = true;
            break;
          }
        }
        assertTrue("Database root.db29 should still exist", db29Exists);
      }

      // Test: Delete alias series database (db32) - should succeed
      statement.execute("DELETE DATABASE root.db32");

      // Verify db32 is deleted
      try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        boolean db32Exists = false;
        while (resultSet.next()) {
          if (resultSet.getString(1).equals("root.db32")) {
            db32Exists = true;
            break;
          }
        }
        assertFalse("Database root.db32 should be deleted", db32Exists);
      }

      // Test: Delete physical series database (db29) again - should succeed now
      statement.execute("DELETE DATABASE root.db29");

      // Verify db29 is deleted
      try (ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        boolean db29Exists = false;
        while (resultSet.next()) {
          if (resultSet.getString(1).equals("root.db29")) {
            db29Exists = true;
            break;
          }
        }
        assertFalse("Database root.db29 should be deleted", db29Exists);
      }

      // Clean up remaining databases
      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE DATABASE root.**");
    }
  }
}
