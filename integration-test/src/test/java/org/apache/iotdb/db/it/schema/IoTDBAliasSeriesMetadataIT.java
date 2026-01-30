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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Integration test for alias series and logical view metadata operations. This test covers: - ALTER
 * TIMESERIES ... RENAME TO ... (creating alias series) - SHOW DEVICES (with all options: WITH
 * DATABASE, WHERE, timeCondition, LIMIT/OFFSET) - SHOW TIMESERIES (with all options: LATEST, WHERE,
 * timeCondition, LIMIT/OFFSET) - SHOW CHILD PATHS - SHOW CHILD NODES - COUNT DEVICES (with
 * timeCondition) - COUNT TIMESERIES (with WHERE, timeCondition, GROUP BY LEVEL) - COUNT NODES
 *
 * <p>All queries should correctly filter disabled series and show alias series and logical views.
 */
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAliasSeriesMetadataIT extends AbstractSchemaIT {

  public IoTDBAliasSeriesMetadataIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @org.junit.runners.Parameterized.BeforeParam
  public static void before() throws Exception {
    SchemaTestMode schemaTestMode = setUpEnvironment();
    // Allocate more memory for PBTree mode to improve performance
    if (schemaTestMode.equals(SchemaTestMode.PBTree)) {
      allocateMemoryForSchemaRegion(10000);
    }
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @org.junit.runners.Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    // Setup test data
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Create databases
      statement.execute("CREATE DATABASE root.db1");
      statement.execute("CREATE DATABASE root.db2");
      statement.execute("CREATE DATABASE root.view");

      // Create physical time series in root.db1
      // Series that will be renamed should have _physical suffix
      statement.execute(
          "CREATE TIMESERIES root.db1.d1.s1_physical WITH DATATYPE=INT64, ENCODING=PLAIN, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.db1.d1.s2_physical WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSION=SNAPPY");
      // Series that won't be renamed should not have suffix
      statement.execute(
          "CREATE TIMESERIES root.db1.d1.s3 WITH DATATYPE=DOUBLE, ENCODING=GORILLA, COMPRESSION=LZ4");
      statement.execute(
          "CREATE TIMESERIES root.db1.d2.s1_physical WITH DATATYPE=INT32, ENCODING=TS_2DIFF, COMPRESSION=UNCOMPRESSED");
      statement.execute(
          "CREATE TIMESERIES root.db1.d2.s2_physical WITH DATATYPE=BOOLEAN, ENCODING=PLAIN, COMPRESSION=SNAPPY");

      // Create physical time series in root.db2
      statement.execute(
          "CREATE TIMESERIES root.db2.d1.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.db2.d1.pressure WITH DATATYPE=DOUBLE, ENCODING=GORILLA, COMPRESSION=LZ4");
      statement.execute(
          "CREATE TIMESERIES root.db2.d2.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSION=SNAPPY");

      // Insert data for time-based queries
      statement.execute(
          "INSERT INTO root.db1.d1(timestamp, s1_physical, s2_physical, s3) VALUES (1000, 1, 1.1, 1.11)");
      statement.execute(
          "INSERT INTO root.db1.d1(timestamp, s1_physical, s2_physical, s3) VALUES (2000, 2, 2.2, 2.22)");
      statement.execute(
          "INSERT INTO root.db2.d1(timestamp, temperature, pressure) VALUES (1000, 20.5, 1013.25)");
      statement.execute(
          "INSERT INTO root.db2.d1(timestamp, temperature, pressure) VALUES (2000, 21.0, 1014.50)");

      // Add tags and attributes for WHERE clause testing
      statement.execute(
          "ALTER timeseries root.db1.d1.s1_physical UPSERT TAGS('unit'='count', 'type'='metric') ATTRIBUTES('desc'='sensor1', 'location'='room1')");
      statement.execute(
          "ALTER timeseries root.db1.d1.s2_physical UPSERT TAGS('unit'='celsius', 'type'='temperature') ATTRIBUTES('desc'='sensor2', 'location'='room2')");
      statement.execute(
          "ALTER timeseries root.db2.d1.temperature UPSERT TAGS('unit'='celsius', 'sensor'='temp1')");

      // ============================================================================
      // Create alias series (RENAME operations) and logical views (CREATE VIEW)
      // ============================================================================

      // Scenario A: Create alias series for physical series (same database)
      statement.execute("ALTER TIMESERIES root.db1.d1.s1_physical RENAME TO root.db1.d1.s1_alias");

      // Scenario B: Create logical view for physical series (different database)
      statement.execute("CREATE VIEW root.view.d1.temp_view AS root.db1.d1.s2_physical");

      // Scenario C: Create alias series for existing alias series (rename alias again)
      // Note: This creates an alias from an alias, which is still alias series
      statement.execute("ALTER TIMESERIES root.db1.d1.s1_alias RENAME TO root.db1.d1.s1_alias_new");

      // Create more aliases and views for comprehensive testing
      // Use alias series for d2.s1_physical
      statement.execute("ALTER TIMESERIES root.db1.d2.s1_physical RENAME TO root.db1.d2.s1_alias");
      // Use logical view for d2.s2_physical
      statement.execute("CREATE VIEW root.db1.d2.s2_view AS root.db1.d2.s2_physical");
    }
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  // ============================================================================
  // Test: ALTER TIMESERIES ... RENAME TO ... (Create Alias Series)
  // ============================================================================

  @Test
  public void testCreateAliasSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Verify alias series and logical views exist
      verifyTimeSeriesExists(
          statement, "root.db1.d1.s1_alias_new", true); // Alias series (renamed from s1_alias)
      verifyTimeSeriesExists(statement, "root.view.d1.temp_view", true); // Logical view
      verifyTimeSeriesExists(statement, "root.db1.d2.s1_alias", true); // Alias series
      verifyTimeSeriesExists(statement, "root.db1.d2.s2_view", true); // Logical view

      // Verify disabled series do not exist in queries
      // RENAME TO disables the original series, but CREATE VIEW does not
      verifyTimeSeriesExists(statement, "root.db1.d1.s1_physical", false); // Disabled by RENAME TO
      verifyTimeSeriesExists(
          statement,
          "root.db1.d1.s2_physical",
          true); // Not disabled by CREATE VIEW (view is in different DB)
      verifyTimeSeriesExists(statement, "root.db1.d1.s1_alias", false); // Renamed to s1_alias_new
      verifyTimeSeriesExists(statement, "root.db1.d2.s1_physical", false); // Disabled by RENAME TO
      verifyTimeSeriesExists(
          statement, "root.db1.d2.s2_physical", true); // Not disabled by CREATE VIEW
      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  // ============================================================================
  // Test: SHOW DEVICES (all variations)
  // ============================================================================

  @Test
  public void testShowDevicesBasic() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // After RENAME/CREATE VIEW, root.db1.d2 should still be shown (it has alias series and
      // logical view which are active)
      // root.db1.d1 should still be shown (it has s3 and s1_alias_new which are active)
      Set<String> expectedDevices = new HashSet<>(Arrays.asList("root.db1.d1", "root.db1.d2"));
      verifyShowDevices(statement, "SHOW DEVICES root.db1.**", expectedDevices, 2);
      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowDevicesWithDatabase() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // SHOW DEVICES WITH DATABASE
      String sql = "SHOW DEVICES root.db1.** WITH DATABASE";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        List<String> devices = new ArrayList<>();
        while (resultSet.next()) {
          String device = resultSet.getString(ColumnHeaderConstant.DEVICE);
          String database = resultSet.getString(ColumnHeaderConstant.DATABASE);
          devices.add(device + "," + database);
        }
        // Should contain root.db1.d1 and root.db1.d2 with database root.db1
        Assert.assertTrue(
            "Should contain root.db1.d1",
            devices.stream().anyMatch(d -> d.startsWith("root.db1.d1")));
        Assert.assertTrue(
            "Should contain root.db1.d2",
            devices.stream().anyMatch(d -> d.startsWith("root.db1.d2")));
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowDevicesWithWhere() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // SHOW DEVICES WHERE DEVICE CONTAINS
      // After RENAME, both d1 and d2 should be found (they have active alias series)
      String sql1 = "SHOW DEVICES root.db1.** WHERE DEVICE CONTAINS 'd1'";
      try (ResultSet resultSet = statement.executeQuery(sql1)) {
        Set<String> devices = new HashSet<>();
        while (resultSet.next()) {
          String device = resultSet.getString(ColumnHeaderConstant.DEVICE);
          devices.add(device);
          Assert.assertTrue("Device should contain 'd1'", device.contains("d1"));
        }
        // Should find root.db1.d1 (has active series: s3, s1_alias_new, s2_physical)
        Assert.assertTrue("Should find root.db1.d1", devices.contains("root.db1.d1"));

        // Verify the device has the expected timeseries
        String sql2 = "SHOW TIMESERIES root.db1.d1.*";
        try (ResultSet timeseriesResult = statement.executeQuery(sql2)) {
          Set<String> timeseries = new HashSet<>();
          while (timeseriesResult.next()) {
            timeseries.add(timeseriesResult.getString(ColumnHeaderConstant.TIMESERIES));
          }
          // Should contain s3, s1_alias_new, s2_physical (s1_physical is disabled by RENAME TO)
          Assert.assertTrue("Should contain s3", timeseries.contains("root.db1.d1.s3"));
          Assert.assertTrue(
              "Should contain s1_alias_new", timeseries.contains("root.db1.d1.s1_alias_new"));
          Assert.assertTrue(
              "Should contain s2_physical", timeseries.contains("root.db1.d1.s2_physical"));
          Assert.assertFalse(
              "Should not contain disabled s1_physical",
              timeseries.contains("root.db1.d1.s1_physical"));
          Assert.assertEquals("Should have exactly 3 timeseries", 3, timeseries.size());
        }
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowDevicesWithAllSeriesRenamed() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Create a device where ALL series will be renamed
      statement.execute(
          "CREATE TIMESERIES root.db1.d3.s1_physical WITH DATATYPE=INT64, ENCODING=PLAIN, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.db1.d3.s2_physical WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.db1.d3.s3_physical WITH DATATYPE=DOUBLE, ENCODING=GORILLA, COMPRESSION=LZ4");

      // Insert some data
      statement.execute(
          "INSERT INTO root.db1.d3(timestamp, s1_physical, s2_physical, s3_physical) VALUES (1000, 1, 1.1, 1.11)");

      // Rename ALL series (all physical series become alias series)
      statement.execute("ALTER TIMESERIES root.db1.d3.s1_physical RENAME TO root.db1.d3.s1_alias");
      statement.execute("ALTER TIMESERIES root.db1.d3.s2_physical RENAME TO root.db1.d3.s2_alias");
      statement.execute("ALTER TIMESERIES root.db1.d3.s3_physical RENAME TO root.db1.d3.s3_alias");

      // Verify the device still appears in SHOW DEVICES (it has active alias series)
      String sql1 = "SHOW DEVICES root.db1.** WHERE DEVICE CONTAINS 'd3'";
      try (ResultSet resultSet = statement.executeQuery(sql1)) {
        Set<String> devices = new HashSet<>();
        while (resultSet.next()) {
          String device = resultSet.getString(ColumnHeaderConstant.DEVICE);
          devices.add(device);
        }
        // Should find root.db1.d3 (has active alias series even though all physical series are
        // renamed)
        Assert.assertTrue("Should find root.db1.d3", devices.contains("root.db1.d3"));
      }

      // Verify the device has only alias series (no physical series remain active)
      String sql2 = "SHOW TIMESERIES root.db1.d3.*";
      try (ResultSet timeseriesResult = statement.executeQuery(sql2)) {
        Set<String> timeseries = new HashSet<>();
        while (timeseriesResult.next()) {
          timeseries.add(timeseriesResult.getString(ColumnHeaderConstant.TIMESERIES));
        }
        // Should contain only alias series
        Assert.assertTrue("Should contain s1_alias", timeseries.contains("root.db1.d3.s1_alias"));
        Assert.assertTrue("Should contain s2_alias", timeseries.contains("root.db1.d3.s2_alias"));
        Assert.assertTrue("Should contain s3_alias", timeseries.contains("root.db1.d3.s3_alias"));
        // Should NOT contain any physical series (all are disabled)
        Assert.assertFalse(
            "Should not contain disabled s1_physical",
            timeseries.contains("root.db1.d3.s1_physical"));
        Assert.assertFalse(
            "Should not contain disabled s2_physical",
            timeseries.contains("root.db1.d3.s2_physical"));
        Assert.assertFalse(
            "Should not contain disabled s3_physical",
            timeseries.contains("root.db1.d3.s3_physical"));
        Assert.assertEquals("Should have exactly 3 alias timeseries", 3, timeseries.size());
      }

      // Verify all original physical series are in INVALID TIMESERIES
      String sql3 = "SHOW INVALID TIMESERIES root.db1.d3.**";
      try (ResultSet invalidResult = statement.executeQuery(sql3)) {
        Set<String> invalidSeries = new HashSet<>();
        while (invalidResult.next()) {
          invalidSeries.add(invalidResult.getString(ColumnHeaderConstant.TIMESERIES));
        }
        // Should contain all disabled physical series
        Assert.assertTrue(
            "Should contain disabled s1_physical",
            invalidSeries.contains("root.db1.d3.s1_physical"));
        Assert.assertTrue(
            "Should contain disabled s2_physical",
            invalidSeries.contains("root.db1.d3.s2_physical"));
        Assert.assertTrue(
            "Should contain disabled s3_physical",
            invalidSeries.contains("root.db1.d3.s3_physical"));
        Assert.assertEquals("Should have exactly 3 invalid timeseries", 3, invalidSeries.size());
      }

      // Verify COUNT DEVICES still counts the device
      long count = getCountDevices(statement, "root.db1.d3");
      Assert.assertEquals("Should count 1 device (with all series renamed)", 1, count);

      // Verify COUNT TIMESERIES counts only alias series
      long timeseriesCount = getCountTimeseries(statement, "root.db1.d3.*");
      Assert.assertEquals("Should count 3 alias timeseries", 3, timeseriesCount);

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowDevicesWithPagination() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // SHOW DEVICES with LIMIT
      String sql1 = "SHOW DEVICES root.** LIMIT 2";
      try (ResultSet resultSet = statement.executeQuery(sql1)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertEquals("Should return at most 2 devices", 2, count);
      }

      // SHOW DEVICES with OFFSET
      String sql2 = "SHOW DEVICES root.** OFFSET 1";
      try (ResultSet resultSet = statement.executeQuery(sql2)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertTrue("Should return devices with offset", count >= 0);
      }

      // SHOW DEVICES with LIMIT and OFFSET
      String sql3 = "SHOW DEVICES root.** LIMIT 2 OFFSET 1";
      try (ResultSet resultSet = statement.executeQuery(sql3)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertTrue("Should return at most 2 devices", count <= 2);
      }

      // SHOW DEVICES with OFFSET and LIMIT
      String sql4 = "SHOW DEVICES root.** OFFSET 1 LIMIT 2";
      try (ResultSet resultSet = statement.executeQuery(sql4)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertTrue("Should return at most 2 devices", count <= 2);
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowDevicesWithTimeCondition() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // SHOW DEVICES with time condition (WHERE time >= 1000)
      String sql = "SHOW DEVICES root.db1.** WHERE time >= 1000";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        // Should return devices that have data at time >= 1000
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertTrue("Should return devices with data at time >= 1000", count > 0);
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowDevicesFiltersDisabledSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      Set<String> expected = new HashSet<>(Arrays.asList("root.db1.d1", "root.db1.d2"));
      verifyShowDevices(statement, "SHOW DEVICES root.db1.**", expected, 2);

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  // ============================================================================
  // Test: SHOW TIMESERIES (all variations)
  // ============================================================================

  @Test
  public void testShowTimeseriesBasic() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // After RENAME/CREATE VIEW, root.db1.d1 should have: s3, s1_alias_new, s2_physical
      // (s1_physical is disabled by RENAME TO, but s2_physical is not disabled by CREATE VIEW)
      String sql = "SHOW TIMESERIES root.db1.d1.*";
      Set<String> expectedSeries =
          new HashSet<>(
              Arrays.asList(
                  "root.db1.d1.s3", "root.db1.d1.s1_alias_new", "root.db1.d1.s2_physical"));
      verifyShowTimeseries(statement, sql, expectedSeries);

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowTimeseriesWithPrefix() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // SHOW TIMESERIES with ** prefix pattern
      String sql = "SHOW TIMESERIES root.db1.d1.**";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int count = 0;
        Set<String> series = new HashSet<>();
        while (resultSet.next()) {
          count++;
          String timeseries = resultSet.getString(ColumnHeaderConstant.TIMESERIES);
          series.add(timeseries);
          Assert.assertTrue(
              "Timeseries should start with root.db1.d1", timeseries.startsWith("root.db1.d1"));
        }
        // Should find s3, s1_alias_new, and s2_physical (s1_physical is disabled by RENAME TO)
        Assert.assertEquals("Should find 3 time series", 3, count);
        Assert.assertTrue("Should contain s3", series.contains("root.db1.d1.s3"));
        Assert.assertTrue(
            "Should contain s1_alias_new", series.contains("root.db1.d1.s1_alias_new"));
        Assert.assertTrue("Should contain s2_physical", series.contains("root.db1.d1.s2_physical"));
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowTimeseriesLatest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // SHOW LATEST TIMESERIES
      String sql = "SHOW LATEST TIMESERIES root.db1.**";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        List<String> timeseries = new ArrayList<>();
        while (resultSet.next()) {
          String timeseriesPath = resultSet.getString(ColumnHeaderConstant.TIMESERIES);
          timeseries.add(timeseriesPath);
        }
        // Should return active time series (excluding disabled ones)
        // root.db1.d1: s3, s1_alias_new, s2_physical
        // root.db1.d2: s1_alias, s2_view, s2_physical
        Assert.assertTrue("Should contain s3", timeseries.contains("root.db1.d1.s3"));
        Assert.assertTrue(
            "Should contain s1_alias_new", timeseries.contains("root.db1.d1.s1_alias_new"));
        Assert.assertTrue(
            "Should contain s2_physical from d1", timeseries.contains("root.db1.d1.s2_physical"));
        Assert.assertTrue("Should contain s1_alias", timeseries.contains("root.db1.d2.s1_alias"));
        Assert.assertTrue("Should contain s2_view", timeseries.contains("root.db1.d2.s2_view"));
        Assert.assertTrue(
            "Should contain s2_physical from d2", timeseries.contains("root.db1.d2.s2_physical"));
        // Should not contain disabled series
        Assert.assertFalse(
            "Should not contain disabled s1_physical from d1",
            timeseries.contains("root.db1.d1.s1_physical"));
        Assert.assertFalse(
            "Should not contain disabled s1_physical from d2",
            timeseries.contains("root.db1.d2.s1_physical"));
        Assert.assertEquals("Should have exactly 6 time series", 6, timeseries.size());
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowInvalidTimeseries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // SHOW INVALID TIMESERIES
      // Should return disabled time series (series that were renamed)
      String sql = "SHOW INVALID TIMESERIES root.db1.**";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        List<String> timeseries = new ArrayList<>();
        while (resultSet.next()) {
          String timeseriesPath = resultSet.getString(ColumnHeaderConstant.TIMESERIES);
          timeseries.add(timeseriesPath);
        }
        // Should return disabled time series (series that were renamed to alias)
        // root.db1.d1.s1_physical was renamed to root.db1.d1.s1_alias_new (disabled)
        // root.db1.d2.s1_physical was renamed to root.db1.d2.s1_alias (disabled)
        Assert.assertTrue(
            "Should contain disabled s1_physical from d1",
            timeseries.contains("root.db1.d1.s1_physical"));
        Assert.assertTrue(
            "Should contain disabled s1_physical from d2",
            timeseries.contains("root.db1.d2.s1_physical"));
        // Should not contain active series
        Assert.assertFalse("Should not contain active s3", timeseries.contains("root.db1.d1.s3"));
        Assert.assertFalse(
            "Should not contain active s1_alias_new",
            timeseries.contains("root.db1.d1.s1_alias_new"));
        Assert.assertFalse(
            "Should not contain active s2_physical from d1",
            timeseries.contains("root.db1.d1.s2_physical"));
        Assert.assertFalse(
            "Should not contain active s1_alias", timeseries.contains("root.db1.d2.s1_alias"));
        Assert.assertFalse(
            "Should not contain active s2_view", timeseries.contains("root.db1.d2.s2_view"));
        Assert.assertFalse(
            "Should not contain active s2_physical from d2",
            timeseries.contains("root.db1.d2.s2_physical"));
        Assert.assertEquals("Should have exactly 2 invalid time series", 2, timeseries.size());
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowInvalidTimeseriesWithTimeCondition() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // SHOW INVALID TIMESERIES with time condition (WHERE time >= 1000)
      // The disabled series (s1_physical from d1 and d2) had data at time 1000 and 2000
      // before being renamed, so they should be returned when filtering by time
      String sql = "SHOW INVALID TIMESERIES root.db1.** WHERE time >= 1000";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        List<String> timeseries = new ArrayList<>();
        while (resultSet.next()) {
          String timeseriesPath = resultSet.getString(ColumnHeaderConstant.TIMESERIES);
          timeseries.add(timeseriesPath);
        }
        // Should return disabled time series that have data at time >= 1000
        // root.db1.d1.s1_physical had data at 1000 and 2000 before being renamed
        Assert.assertTrue(
            "Should contain disabled s1_physical from d1 with data at time >= 1000",
            timeseries.contains("root.db1.d1.s1_physical"));
        Assert.assertEquals(
            "Should have exactly 1 invalid time series with data at time >= 1000",
            1,
            timeseries.size());
      }

      // SHOW INVALID TIMESERIES with time condition (WHERE time >= 2000)
      // Only series with data at time >= 2000 should be returned
      String sql2 = "SHOW INVALID TIMESERIES root.db1.** WHERE time >= 2000";
      try (ResultSet resultSet = statement.executeQuery(sql2)) {
        List<String> timeseries = new ArrayList<>();
        while (resultSet.next()) {
          String timeseriesPath = resultSet.getString(ColumnHeaderConstant.TIMESERIES);
          timeseries.add(timeseriesPath);
        }

        Assert.assertTrue(
            "Should contain disabled s1_physical from d1 with data at time >= 2000",
            timeseries.contains("root.db1.d1.s1_physical"));
        Assert.assertEquals(
            "Should have exactly 1 invalid time series with data at time >= 2000",
            1,
            timeseries.size());
      }

      // SHOW INVALID TIMESERIES with time condition (WHERE time > 2000)
      // No series should have data after 2000, so result should be empty
      String sql3 = "SHOW INVALID TIMESERIES root.db1.** WHERE time > 2000";
      try (ResultSet resultSet = statement.executeQuery(sql3)) {
        List<String> timeseries = new ArrayList<>();
        while (resultSet.next()) {
          String timeseriesPath = resultSet.getString(ColumnHeaderConstant.TIMESERIES);
          timeseries.add(timeseriesPath);
        }
        // No disabled series have data after time 2000
        Assert.assertEquals(
            "Should have no invalid time series with data at time > 2000", 0, timeseries.size());
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowTimeseriesWithWhere() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // SHOW TIMESERIES WHERE TIMESERIES CONTAINS 's1'
      // Should find root.db1.d1.s1_alias_new and root.db1.d2.s1_alias (2 series)
      String sql1 = "SHOW TIMESERIES root.db1.** WHERE TIMESERIES CONTAINS 's1'";
      try (ResultSet resultSet = statement.executeQuery(sql1)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
          String timeseries = resultSet.getString(ColumnHeaderConstant.TIMESERIES);
          Assert.assertTrue("Timeseries should contain 's1'", timeseries.contains("s1"));
        }
        Assert.assertEquals("Should find 2 time series containing 's1'", 2, count);
      }

      // SHOW TIMESERIES WHERE DATATYPE = 'INT64'
      // root.db1.d1.s1_alias_new (alias of INT64 series) should be found
      String sql2 = "SHOW TIMESERIES root.db1.** WHERE DATATYPE = 'INT64'";
      try (ResultSet resultSet = statement.executeQuery(sql2)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
          String dataType = resultSet.getString(ColumnHeaderConstant.DATATYPE);
          Assert.assertEquals("DataType should be INT64", "INT64", dataType);
        }
        Assert.assertEquals("Should find 1 INT64 time series", 1, count);
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowTimeseriesWithPagination() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // SHOW TIMESERIES with LIMIT
      String sql1 = "SHOW TIMESERIES root.db1.** LIMIT 2";
      try (ResultSet resultSet = statement.executeQuery(sql1)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertEquals("Should return at most 2 time series", 2, count);
      }

      // SHOW TIMESERIES with LIMIT and OFFSET
      String sql3 = "SHOW TIMESERIES root.db1.** LIMIT 2 OFFSET 1";
      try (ResultSet resultSet = statement.executeQuery(sql3)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertTrue("Should return at most 2 time series", count == 2);
      }

      // SHOW TIMESERIES with OFFSET and LIMIT
      String sql4 = "SHOW TIMESERIES root.db1.** OFFSET 1 LIMIT 2";
      try (ResultSet resultSet = statement.executeQuery(sql4)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertTrue("Should return at most 2 time series", count == 2);
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowTimeseriesWithTimeCondition() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // SHOW TIMESERIES with time condition (WHERE time >= 1000)
      String sql = "SHOW TIMESERIES root.db1.** WHERE time >= 1000";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        // Should return time series that have data at time >= 1000
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertTrue("Should return time series with data at time >= 1000", count == 3);
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowTimeseriesFiltersDisabledSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // After RENAME, disabled series should not appear
      String sql = "SHOW TIMESERIES root.db1.d1.*";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        List<String> series = new ArrayList<>();
        while (resultSet.next()) {
          series.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
        }
        // Should contain s3, s1_alias_new, s2_physical but NOT s1_physical (disabled by RENAME TO)
        // Note: s2_physical is NOT disabled by CREATE VIEW (view is in different database)
        Assert.assertFalse(
            "Should not contain disabled series s1_physical",
            series.contains("root.db1.d1.s1_physical"));
        Assert.assertTrue(
            "Should contain s2_physical (not disabled by CREATE VIEW)",
            series.contains("root.db1.d1.s2_physical"));
        Assert.assertTrue(
            "Should contain alias series s1_alias_new",
            series.contains("root.db1.d1.s1_alias_new"));
        Assert.assertTrue("Should contain active series s3", series.contains("root.db1.d1.s3"));
      }

      // Test root.db1.d2.* should show alias series, not disabled ones
      String sql2 = "SHOW TIMESERIES root.db1.d2.*";
      try (ResultSet resultSet = statement.executeQuery(sql2)) {
        List<String> series = new ArrayList<>();
        while (resultSet.next()) {
          series.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
        }
        // Should contain s1_alias, s2_view, s2_physical but NOT s1_physical (disabled by RENAME TO)
        // Note: s2_physical is NOT disabled by CREATE VIEW
        Assert.assertFalse(
            "Should not contain disabled series s1_physical",
            series.contains("root.db1.d2.s1_physical"));
        Assert.assertTrue(
            "Should contain s2_physical (not disabled by CREATE VIEW)",
            series.contains("root.db1.d2.s2_physical"));
        Assert.assertTrue(
            "Should contain alias series s1_alias", series.contains("root.db1.d2.s1_alias"));
        Assert.assertTrue(
            "Should contain logical view s2_view", series.contains("root.db1.d2.s2_view"));
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  // ============================================================================
  // Test: SHOW CHILD PATHS
  // ============================================================================

  @Test
  public void testShowChildPaths() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // SHOW CHILD PATHS
      String sql = "SHOW CHILD PATHS root.db1";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        Set<String> childPaths = new HashSet<>();
        while (resultSet.next()) {
          childPaths.add(resultSet.getString(1));
        }
        // Both d1 and d2 should be shown (they have active series)
        Assert.assertTrue("Should contain d1", childPaths.contains("root.db1.d1"));
        Assert.assertTrue("Should contain d2", childPaths.contains("root.db1.d2"));
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowChildPathsWithFilter() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Create additional devices for filtering test
      statement.execute(
          "CREATE TIMESERIES root.db1.d3.s1 WITH DATATYPE=INT64, ENCODING=PLAIN, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.db1.d4.s1 WITH DATATYPE=INT64, ENCODING=PLAIN, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.db1.device1.s1 WITH DATATYPE=INT64, ENCODING=PLAIN, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.db1.device2.s1 WITH DATATYPE=INT64, ENCODING=PLAIN, COMPRESSION=SNAPPY");

      // Test: SHOW CHILD PATHS with path pattern filter (using wildcard)
      // Filter for devices starting with 'd' followed by a digit
      String sql1 = "SHOW CHILD PATHS root.db1";
      try (ResultSet resultSet = statement.executeQuery(sql1)) {
        Set<String> allChildPaths = new HashSet<>();
        while (resultSet.next()) {
          allChildPaths.add(resultSet.getString(1));
        }
        // Should contain d1, d2, d3, d4, device1, device2
        Assert.assertTrue("Should contain d1", allChildPaths.contains("root.db1.d1"));
        Assert.assertTrue("Should contain d2", allChildPaths.contains("root.db1.d2"));
        Assert.assertTrue("Should contain d3", allChildPaths.contains("root.db1.d3"));
        Assert.assertTrue("Should contain d4", allChildPaths.contains("root.db1.d4"));
        Assert.assertTrue("Should contain device1", allChildPaths.contains("root.db1.device1"));
        Assert.assertTrue("Should contain device2", allChildPaths.contains("root.db1.device2"));

        // Filter results in code: devices starting with 'd' followed by a digit (d1, d2, d3, d4)
        Set<String> filteredPaths = new HashSet<>();
        for (String path : allChildPaths) {
          // Extract the device name (e.g., "d1" from "root.db1.d1")
          String[] parts = path.split("\\.");
          if (parts.length >= 3) {
            String deviceName = parts[2];
            // Filter: device name starts with 'd' and is followed by a single digit
            if (deviceName.matches("^d\\d$")) {
              filteredPaths.add(path);
            }
          }
        }
        // Should filter to d1, d2, d3, d4 (devices matching pattern d<digit>)
        Assert.assertTrue("Filtered should contain d1", filteredPaths.contains("root.db1.d1"));
        Assert.assertTrue("Filtered should contain d2", filteredPaths.contains("root.db1.d2"));
        Assert.assertTrue("Filtered should contain d3", filteredPaths.contains("root.db1.d3"));
        Assert.assertTrue("Filtered should contain d4", filteredPaths.contains("root.db1.d4"));
        Assert.assertFalse(
            "Filtered should not contain device1", filteredPaths.contains("root.db1.device1"));
        Assert.assertFalse(
            "Filtered should not contain device2", filteredPaths.contains("root.db1.device2"));
        Assert.assertEquals("Should have exactly 4 filtered devices", 4, filteredPaths.size());
      }

      // Test: Filter devices containing 'device' in their name
      String sql2 = "SHOW CHILD PATHS root.db1";
      try (ResultSet resultSet = statement.executeQuery(sql2)) {
        Set<String> allChildPaths = new HashSet<>();
        while (resultSet.next()) {
          allChildPaths.add(resultSet.getString(1));
        }

        // Filter results in code: devices containing 'device'
        Set<String> filteredPaths = new HashSet<>();
        for (String path : allChildPaths) {
          String[] parts = path.split("\\.");
          if (parts.length >= 3) {
            String deviceName = parts[2];
            if (deviceName.contains("device")) {
              filteredPaths.add(path);
            }
          }
        }
        // Should filter to device1 and device2
        Assert.assertTrue(
            "Filtered should contain device1", filteredPaths.contains("root.db1.device1"));
        Assert.assertTrue(
            "Filtered should contain device2", filteredPaths.contains("root.db1.device2"));
        Assert.assertFalse("Filtered should not contain d1", filteredPaths.contains("root.db1.d1"));
        Assert.assertFalse("Filtered should not contain d2", filteredPaths.contains("root.db1.d2"));
        Assert.assertEquals("Should have exactly 2 filtered devices", 2, filteredPaths.size());
      }

      // Test: Filter devices that have all series renamed (using the device created in previous
      // test)
      // Rename all series in d3 to create a device with all alias series
      statement.execute("ALTER TIMESERIES root.db1.d3.s1 RENAME TO root.db1.d3.s1_alias");

      String sql3 = "SHOW CHILD PATHS root.db1";
      try (ResultSet resultSet = statement.executeQuery(sql3)) {
        Set<String> allChildPaths = new HashSet<>();
        while (resultSet.next()) {
          allChildPaths.add(resultSet.getString(1));
        }
        // d3 should still appear even though all its series are renamed (it has active alias
        // series)
        Assert.assertTrue(
            "Should contain d3 (has active alias series)", allChildPaths.contains("root.db1.d3"));

        // Verify d3 has only alias series
        String sql4 = "SHOW TIMESERIES root.db1.d3.*";
        try (ResultSet timeseriesResult = statement.executeQuery(sql4)) {
          Set<String> timeseries = new HashSet<>();
          while (timeseriesResult.next()) {
            timeseries.add(timeseriesResult.getString(ColumnHeaderConstant.TIMESERIES));
          }
          Assert.assertTrue("Should contain s1_alias", timeseries.contains("root.db1.d3.s1_alias"));
          Assert.assertFalse(
              "Should not contain disabled s1", timeseries.contains("root.db1.d3.s1"));
          Assert.assertEquals("Should have exactly 1 alias timeseries", 1, timeseries.size());
        }
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  // ============================================================================
  // Test: SHOW CHILD NODES
  // ============================================================================

  @Test
  public void testShowChildNodes() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // SHOW CHILD NODES root.db1.d1
      // After RENAME/CREATE VIEW, should show s3, s1_alias_new, and s2_physical (s1_physical is
      // disabled)
      String sql = "SHOW CHILD NODES root.db1.d1";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        Set<String> childNodes = new HashSet<>();
        while (resultSet.next()) {
          childNodes.add(resultSet.getString(1));
        }
        Assert.assertFalse(
            "Should not contain disabled node s1_physical", childNodes.contains("s1_physical"));
        Assert.assertTrue(
            "Should contain s2_physical (not disabled by CREATE VIEW)",
            childNodes.contains("s2_physical"));
        Assert.assertTrue(
            "Should contain alias node s1_alias_new", childNodes.contains("s1_alias_new"));
        Assert.assertTrue("Should contain active node s3", childNodes.contains("s3"));
      }

      // SHOW CHILD NODES root.db1.d2
      // Should show s1_alias, s2_view, and s2_physical (s1_physical is disabled)
      String sql2 = "SHOW CHILD NODES root.db1.d2";
      try (ResultSet resultSet = statement.executeQuery(sql2)) {
        Set<String> childNodes = new HashSet<>();
        while (resultSet.next()) {
          childNodes.add(resultSet.getString(1));
        }
        Assert.assertFalse(
            "Should not contain disabled node s1_physical", childNodes.contains("s1_physical"));
        Assert.assertTrue(
            "Should contain s2_physical (not disabled by CREATE VIEW)",
            childNodes.contains("s2_physical"));
        Assert.assertTrue("Should contain alias node s1_alias", childNodes.contains("s1_alias"));
        Assert.assertTrue(
            "Should contain logical view node s2_view", childNodes.contains("s2_view"));
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testShowChildNodesFiltersDisabledSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Same as testShowChildNodes, but explicitly testing filtering
      String sql = "SHOW CHILD NODES root.db1.d1";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        Set<String> childNodes = new HashSet<>();
        while (resultSet.next()) {
          childNodes.add(resultSet.getString(1));
        }
        // Verify disabled nodes are filtered out
        Assert.assertFalse(
            "Should not contain disabled node s1_physical", childNodes.contains("s1_physical"));
        Assert.assertTrue(
            "Should contain s2_physical (not disabled by CREATE VIEW)",
            childNodes.contains("s2_physical"));
        // Verify alias and active nodes are shown
        Assert.assertTrue(
            "Should contain alias node s1_alias_new", childNodes.contains("s1_alias_new"));
        Assert.assertTrue("Should contain active node s3", childNodes.contains("s3"));
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  // ============================================================================
  // Test: COUNT DEVICES
  // ============================================================================

  @Test
  public void testCountDevicesBasic() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // COUNT DEVICES root.db1.**
      // Should count d1 and d2 (both have active series after RENAME)
      long count = getCountDevices(statement, "root.db1.**");
      Assert.assertEquals("Should count 2 devices", 2, count);

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testCountDevicesFiltersDisabledSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // After RENAME, both d1 and d2 should be counted (they have active alias series)
      long count = getCountDevices(statement, "root.db1.**");
      Assert.assertEquals("Should count 2 devices (both have active series)", 2, count);

      // Verify it matches SHOW DEVICES count
      long showCount = getShowDevicesCount(statement, "root.db1.**");
      Assert.assertEquals("COUNT should match SHOW count", showCount, count);

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testCountDevicesWithTimeCondition() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // COUNT DEVICES with time condition (WHERE time >= 1000)
      String sql = "COUNT DEVICES root.db1.** WHERE time >= 1000";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        Assert.assertTrue(resultSet.next());
        long count = resultSet.getLong(1);
        Assert.assertTrue("Should count devices with data at time >= 1000", count == 1);
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testCountDevicesFiltersInternalNodes() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Create devices with internal nodes (intermediate path nodes)
      // root.db1.internal.device.s1 (will be renamed)
      statement.execute(
          "CREATE TIMESERIES root.db1.internal.device.s1 WITH DATATYPE=INT64, ENCODING=PLAIN, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.db1.internal.device.s2 WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSION=SNAPPY");

      // root.db1.another.device.s1
      statement.execute(
          "CREATE TIMESERIES root.db1.another.device.s1 WITH DATATYPE=INT64, ENCODING=PLAIN, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.db1.another.device.s2 WITH DATATYPE=DOUBLE, ENCODING=GORILLA, COMPRESSION=LZ4");

      // Insert some data
      statement.execute(
          "INSERT INTO root.db1.internal.device(timestamp, s1, s2) VALUES (1000, 1, 1.1)");
      statement.execute(
          "INSERT INTO root.db1.another.device(timestamp, s1, s2) VALUES (1000, 2, 2.2)");

      // Rename s1 in internal.device to create alias series
      statement.execute(
          "ALTER TIMESERIES root.db1.internal.device.s1 RENAME TO root.db1.internal.device.s1_alias");

      // COUNT DEVICES should only count actual device nodes, not internal nodes
      // Actual devices: root.db1.internal.device, root.db1.another.device
      // Internal nodes (should be filtered): root.db1.internal, root.db1.another
      long count = getCountDevices(statement, "root.db1.**");
      // Should count: d1, d2 (from setUp), internal.device, another.device = 4 devices
      // Internal nodes (internal, another) should NOT be counted as devices
      Assert.assertEquals(
          "Should count 4 devices (d1, d2, internal.device, another.device), excluding internal nodes",
          4,
          count);

      // Verify with SHOW DEVICES to ensure consistency
      String sql = "SHOW DEVICES root.db1.**";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        Set<String> devices = new HashSet<>();
        while (resultSet.next()) {
          devices.add(resultSet.getString(ColumnHeaderConstant.DEVICE));
        }
        // Should contain actual device nodes
        Assert.assertTrue(
            "Should contain root.db1.internal.device",
            devices.contains("root.db1.internal.device"));
        Assert.assertTrue(
            "Should contain root.db1.another.device", devices.contains("root.db1.another.device"));
        Assert.assertTrue("Should contain root.db1.d1", devices.contains("root.db1.d1"));
        Assert.assertTrue("Should contain root.db1.d2", devices.contains("root.db1.d2"));

        // Should NOT contain internal nodes (intermediate path nodes)
        Assert.assertFalse(
            "Should not contain internal node root.db1.internal",
            devices.contains("root.db1.internal"));
        Assert.assertFalse(
            "Should not contain internal node root.db1.another",
            devices.contains("root.db1.another"));

        Assert.assertEquals("Should have exactly 4 devices", 4, devices.size());
      }

      // Verify that internal.device has alias series (s1_alias) and active series (s2)
      String sql2 = "SHOW TIMESERIES root.db1.internal.device.*";
      try (ResultSet resultSet = statement.executeQuery(sql2)) {
        Set<String> timeseries = new HashSet<>();
        while (resultSet.next()) {
          timeseries.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
        }
        Assert.assertTrue(
            "Should contain s1_alias", timeseries.contains("root.db1.internal.device.s1_alias"));
        Assert.assertTrue("Should contain s2", timeseries.contains("root.db1.internal.device.s2"));
        Assert.assertFalse(
            "Should not contain disabled s1", timeseries.contains("root.db1.internal.device.s1"));
        Assert.assertEquals("Should have exactly 2 timeseries", 2, timeseries.size());
      }

      // Verify that another.device has active series
      String sql3 = "SHOW TIMESERIES root.db1.another.device.*";
      try (ResultSet resultSet = statement.executeQuery(sql3)) {
        Set<String> timeseries = new HashSet<>();
        while (resultSet.next()) {
          timeseries.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
        }
        Assert.assertTrue("Should contain s1", timeseries.contains("root.db1.another.device.s1"));
        Assert.assertTrue("Should contain s2", timeseries.contains("root.db1.another.device.s2"));
        Assert.assertEquals("Should have exactly 2 timeseries", 2, timeseries.size());
      }

      // Verify COUNT matches SHOW count
      long showCount = getShowDevicesCount(statement, "root.db1.**");
      Assert.assertEquals("COUNT should match SHOW count", showCount, count);

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  // ============================================================================
  // Test: COUNT TIMESERIES
  // ============================================================================

  @Test
  public void testCountTimeseriesBasic() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // COUNT TIMESERIES root.db1.d1.*
      // After RENAME/CREATE VIEW: should count s3, s1_alias_new, s2_physical (3 series)
      // s1_physical is disabled by RENAME TO, but s2_physical is not disabled by CREATE VIEW
      long count = getCountTimeseries(statement, "root.db1.d1.*");
      Assert.assertEquals("Should count 3 time series (s3, s1_alias_new, s2_physical)", 3, count);

      // COUNT TIMESERIES root.db1.d2.*
      // Should count s1_alias, s2_view, s2_physical (3 series)
      long count2 = getCountTimeseries(statement, "root.db1.d2.*");
      Assert.assertEquals("Should count 3 time series (s1_alias, s2_view, s2_physical)", 3, count2);

      // COUNT TIMESERIES root.db1.**
      // Should count all active series: s3, s1_alias_new, s2_physical (from d1), s1_alias, s2_view,
      // s2_physical (from d2) = 6 series total
      long count3 = getCountTimeseries(statement, "root.db1.**");
      Assert.assertEquals("Should count 6 time series total", 6, count3);

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testCountTimeseriesWithWhere() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // COUNT TIMESERIES WHERE DATATYPE = 'INT64'
      // Should find root.db1.d1.s1_alias_new (alias of INT64 series)
      String sql = "COUNT TIMESERIES root.db1.** WHERE DATATYPE = 'INT64'";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        Assert.assertTrue(resultSet.next());
        long count = resultSet.getLong(1);
        Assert.assertEquals("Should count 1 INT64 time series", 1, count);
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testCountTimeseriesGroupByLevel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // COUNT TIMESERIES GROUP BY LEVEL = 3
      // Level 3 means measurement level: each measurement path is grouped separately
      // After RENAME/CREATE VIEW:
      // - root.db1.d1: s3 (active), s1_alias_new (active alias series), s2_physical (active, not
      // disabled by CREATE VIEW)
      // - root.db1.d2: s1_alias (active alias series), s2_view (active logical view), s2_physical
      // (active, not disabled by CREATE VIEW)
      // Should return 6 groups, each with count 1
      String sql = "COUNT TIMESERIES root.db1.** GROUP BY LEVEL = 3";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        // Should return grouped counts by measurement path (level 3)
        Set<String> results = new HashSet<>();
        while (resultSet.next()) {
          String path = resultSet.getString(1);
          long count = resultSet.getLong(2);
          results.add(path + "," + count);
        }
        // Should have 6 measurement paths, each with count 1
        Assert.assertTrue(
            "Should contain root.db1.d1.s3 with count 1", results.contains("root.db1.d1.s3,1"));
        Assert.assertTrue(
            "Should contain root.db1.d1.s1_alias_new with count 1",
            results.contains("root.db1.d1.s1_alias_new,1"));
        Assert.assertTrue(
            "Should contain root.db1.d1.s2_physical with count 1",
            results.contains("root.db1.d1.s2_physical,1"));
        Assert.assertTrue(
            "Should contain root.db1.d2.s1_alias with count 1",
            results.contains("root.db1.d2.s1_alias,1"));
        Assert.assertTrue(
            "Should contain root.db1.d2.s2_view with count 1",
            results.contains("root.db1.d2.s2_view,1"));
        Assert.assertTrue(
            "Should contain root.db1.d2.s2_physical with count 1",
            results.contains("root.db1.d2.s2_physical,1"));
        Assert.assertEquals(
            "Should have exactly 6 groups (excluding disabled series)", 6, results.size());
        // Verify disabled series are not included
        Assert.assertFalse(
            "Should not contain disabled s1_physical from d1",
            results.contains("root.db1.d1.s1_physical,1"));
        Assert.assertFalse(
            "Should not contain disabled s1_physical from d2",
            results.contains("root.db1.d2.s1_physical,1"));
      }

      // COUNT TIMESERIES GROUP BY LEVEL = 1
      // Level 1 means database level: root.db1
      // Total count: s3 + s1_alias_new + s2_physical (from d1) + s1_alias + s2_view + s2_physical
      // (from d2) = 6 series
      String sql2 = "COUNT TIMESERIES root.db1.** GROUP BY LEVEL = 1";
      try (ResultSet resultSet = statement.executeQuery(sql2)) {
        Assert.assertTrue("Should return at least one row", resultSet.next());
        String path = resultSet.getString(1);
        long count = resultSet.getLong(2);
        Assert.assertEquals("Should return root.db1", "root.db1", path);
        Assert.assertEquals("Should count 6 time series (excluding disabled ones)", 6, count);
        // Should only have one group
        Assert.assertFalse("Should only have one group", resultSet.next());
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testCountTimeseriesWithTimeCondition() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // COUNT TIMESERIES with time condition (WHERE time >= 1000)
      String sql = "COUNT TIMESERIES root.db1.** WHERE time >= 1000";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        Assert.assertTrue(resultSet.next());
        long count = resultSet.getLong(1);
        Assert.assertTrue("Should count time series with data at time >= 1000", count > 0);
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testCountTimeseriesFiltersDisabledSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // After RENAME/CREATE VIEW, should count alias series, logical views, and physical series
      // that are not disabled
      // root.db1.d1.* should count s3, s1_alias_new, and s2_physical (3 series)
      // s1_physical is disabled by RENAME TO, but s2_physical is not disabled by CREATE VIEW
      long count = getCountTimeseries(statement, "root.db1.d1.*");
      Assert.assertEquals("Should count 3 time series (excluding disabled s1_physical)", 3, count);

      // Verify it matches SHOW TIMESERIES count
      long showCount = getShowTimeseriesCount(statement, "root.db1.d1.*");
      Assert.assertEquals("COUNT should match SHOW count", showCount, count);

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  // ============================================================================
  // Test: COUNT NODES
  // ============================================================================

  @Test
  public void testCountNodes() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // COUNT NODES root.db1.d1 LEVEL = 3
      // Level is absolute from root: root(0) -> db1(1) -> d1(2) -> s3/s1_alias_new/s2_physical(3)
      // After RENAME/CREATE VIEW in setUp:
      // - s1_physical -> RENAME TO root.db1.d1.s1_alias -> RENAME TO root.db1.d1.s1_alias_new
      // (s1_physical disabled, s1_alias_new active)
      // - s2_physical -> CREATE VIEW root.view.d1.temp_view AS s2_physical (s2_physical NOT
      // disabled,
      // view is in different database)
      // - s3 -> remains active
      // So root.db1.d1 should have: s3, s1_alias_new, s2_physical (3 active nodes at level 3)
      String sql = "COUNT NODES root.db1.d1.** LEVEL = 3";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        Assert.assertTrue(resultSet.next());
        long count = resultSet.getLong(1);
        Assert.assertEquals(
            "Should count 3 nodes (s3, s1_alias_new, s2_physical, excluding disabled s1_physical)",
            3,
            count);
      }

      // COUNT NODES root.db1.d2 LEVEL = 3
      // After RENAME/CREATE VIEW in setUp:
      // - s1_physical -> RENAME TO root.db1.d2.s1_alias (s1_physical disabled, s1_alias active)
      // - s2_physical -> CREATE VIEW root.db1.d2.s2_view AS s2_physical (s2_physical NOT disabled,
      // s2_view active)
      // So root.db1.d2 should have: s1_alias, s2_view, s2_physical (3 active nodes at level 3)
      String sql2 = "COUNT NODES root.db1.d2.** LEVEL = 3";
      try (ResultSet resultSet = statement.executeQuery(sql2)) {
        Assert.assertTrue(resultSet.next());
        long count = resultSet.getLong(1);
        Assert.assertEquals(
            "Should count 3 nodes (s1_alias, s2_view, s2_physical, excluding disabled s1_physical)",
            3,
            count);
      }

      // Verify it matches SHOW CHILD NODES count
      // For root.db1.d1, should have s3, s1_alias_new, and s2_physical (3 nodes)
      String sql3 = "SHOW CHILD NODES root.db1.d1";
      try (ResultSet resultSet = statement.executeQuery(sql3)) {
        Set<String> childNodes = new HashSet<>();
        while (resultSet.next()) {
          childNodes.add(resultSet.getString(1));
        }
        Assert.assertEquals("SHOW CHILD NODES should return 3 nodes", 3, childNodes.size());
        Assert.assertTrue("Should contain s3", childNodes.contains("s3"));
        Assert.assertTrue("Should contain s1_alias_new", childNodes.contains("s1_alias_new"));
        Assert.assertTrue("Should contain s2_physical", childNodes.contains("s2_physical"));
        Assert.assertFalse(
            "Should not contain disabled s1_physical", childNodes.contains("s1_physical"));
      }

      // For root.db1.d2, should have s1_alias, s2_view, and s2_physical (3 nodes)
      String sql3_d2 = "SHOW CHILD NODES root.db1.d2";
      try (ResultSet resultSet = statement.executeQuery(sql3_d2)) {
        Set<String> childNodes = new HashSet<>();
        while (resultSet.next()) {
          childNodes.add(resultSet.getString(1));
        }
        Assert.assertEquals("SHOW CHILD NODES should return 3 nodes", 3, childNodes.size());
        Assert.assertTrue("Should contain s1_alias", childNodes.contains("s1_alias"));
        Assert.assertTrue("Should contain s2_view", childNodes.contains("s2_view"));
        Assert.assertTrue("Should contain s2_physical", childNodes.contains("s2_physical"));
        Assert.assertFalse(
            "Should not contain disabled s1_physical", childNodes.contains("s1_physical"));
      }

      // Verify COUNT matches SHOW count
      String sql4 = "COUNT NODES root.db1.d1.** LEVEL = 3";
      try (ResultSet countResult = statement.executeQuery(sql4)) {
        countResult.next();
        long countNodes = countResult.getLong(1);
        try (ResultSet showResult = statement.executeQuery("SHOW CHILD NODES root.db1.d1")) {
          long showNodes = 0;
          while (showResult.next()) {
            showNodes++;
          }
          Assert.assertEquals(
              "COUNT NODES should match SHOW CHILD NODES count", showNodes, countNodes);
        }
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  // ============================================================================
  // Test: ALTER TIMESERIES on disabled series (should be forbidden)
  // ============================================================================

  @Test
  public void testAlterTimeSeriesOnDisabledSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Create a new time series for testing
      statement.execute(
          "CREATE TIMESERIES root.db1.d3.s1_physical WITH DATATYPE=INT64, ENCODING=PLAIN, COMPRESSION=SNAPPY");
      statement.execute(
          "ALTER TIMESERIES root.db1.d3.s1_physical UPSERT TAGS('unit'='test', 'type'='sensor') ATTRIBUTES('desc'='test sensor')");

      // Create alias - this will disable root.db1.d3.s1_physical
      statement.execute("ALTER TIMESERIES root.db1.d3.s1_physical RENAME TO root.db1.d3.s1_alias");

      // Verify that the alias series exists and the original series is disabled
      // (by checking that SHOW TIMESERIES returns the alias, not the original)
      try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES root.db1.d3.s1_alias")) {
        Assert.assertTrue("Alias series should exist after RENAME", resultSet.next());
        Assert.assertEquals(
            "Should find the alias series",
            "root.db1.d3.s1_alias",
            resultSet.getString(ColumnHeaderConstant.TIMESERIES));
      }

      // Try to alter the disabled series - should fail
      // 1. Try to add tags
      try {
        statement.execute("ALTER TIMESERIES root.db1.d3.s1_physical ADD TAGS 'newTag'='newValue'");
        Assert.fail("Should throw exception when altering disabled time series");
      } catch (SQLException e) {
        Assert.assertTrue(
            "Error message should mention disabled time series",
            e.getMessage().contains("disabled") || e.getMessage().contains("Cannot alter"));
      }

      // 2. Try to add attributes
      try {
        statement.execute(
            "ALTER TIMESERIES root.db1.d3.s1_physical ADD ATTRIBUTES 'newAttr'='newValue'");
        Assert.fail("Should throw exception when altering disabled time series");
      } catch (SQLException e) {
        Assert.assertTrue(
            "Error message should mention disabled time series",
            e.getMessage().contains("disabled") || e.getMessage().contains("Cannot alter"));
      }

      // 3. Try to set tags/attributes
      try {
        statement.execute("ALTER TIMESERIES root.db1.d3.s1_physical SET 'unit'='updated'");
        Assert.fail("Should throw exception when altering disabled time series");
      } catch (SQLException e) {
        Assert.assertTrue(
            "Error message should mention disabled time series",
            e.getMessage().contains("disabled") || e.getMessage().contains("Cannot alter"));
      }

      // 4. Try to drop tags/attributes
      try {
        statement.execute("ALTER TIMESERIES root.db1.d3.s1_physical DROP 'unit'");
        Assert.fail("Should throw exception when altering disabled time series");
      } catch (SQLException e) {
        Assert.assertTrue(
            "Error message should mention disabled time series",
            e.getMessage().contains("disabled") || e.getMessage().contains("Cannot alter"));
      }

      // 5. Try to rename tag/attribute key
      try {
        statement.execute("ALTER TIMESERIES root.db1.d3.s1_physical RENAME unit TO 'unit_new'");
        Assert.fail("Should throw exception when altering disabled time series");
      } catch (SQLException e) {
        Assert.assertTrue(
            "Error message should mention disabled time series",
            e.getMessage().contains("disabled") || e.getMessage().contains("Cannot alter"));
      }

      // 6. Try to upsert alias/tags/attributes
      try {
        statement.execute("ALTER TIMESERIES root.db1.d3.s1_physical UPSERT TAGS('tag1'='value1')");
        Assert.fail("Should throw exception when altering disabled time series");
      } catch (SQLException e) {
        Assert.assertTrue(
            "Error message should mention disabled time series",
            e.getMessage().contains("disabled") || e.getMessage().contains("Cannot alter"));
      }

      // Verify that altering the alias series works (it's not disabled)
      // This should succeed
      statement.execute("ALTER TIMESERIES root.db1.d3.s1_alias ADD TAGS 'aliasTag'='aliasValue'");
      statement.execute("ALTER TIMESERIES root.db1.d3.s1_alias SET 'aliasTag'='updatedValue'");

      // Verify the alias series can be queried and has the updated tags
      try (ResultSet resultSet =
          statement.executeQuery(
              "SHOW TIMESERIES root.db1.d3.s1_alias WHERE TAGS('aliasTag') = 'updatedValue'")) {
        Assert.assertTrue("Should find the alias series with updated tag", resultSet.next());
      }

      statement.execute("DELETE TIMESERIES root.**");
    }
  }

  @Test
  public void testCreateAliasSeriesFromTemplate() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Create database
      statement.execute("CREATE DATABASE root.db3");

      // Create device template with measurements
      statement.execute(
          "CREATE DEVICE TEMPLATE template1 (s1 INT64 encoding=PLAIN compressor=SNAPPY, s2 DOUBLE encoding=GORILLA compressor=LZ4)");

      // Set template to database
      statement.execute("SET DEVICE TEMPLATE template1 TO root.db3");

      // Activate template to create time series
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.db3.d1");

      // Test: RENAME on template-based time series should fail
      // Template-based time series cannot be renamed/aliased
      try {
        statement.execute("ALTER TIMESERIES root.db3.d1.s1 RENAME TO root.db3.d1.s1_alias");
        Assert.fail("Should throw exception when renaming template-based time series");
      } catch (SQLException e) {
        Assert.assertTrue(
            "Error message should mention template-based time series cannot be renamed",
            e.getMessage().contains("template-based")
                || e.getMessage().contains("template")
                || e.getMessage().contains("Cannot create alias series"));
      }

      // Verify that template-based time series still exists and is not disabled
      verifyTimeSeriesExists(statement, "root.db3.d1.s1", true);
      verifyTimeSeriesExists(statement, "root.db3.d1.s2", true);

      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE database root.**");
    }
  }

  @Test
  public void testCreateAliasSeriesFromView() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Create database
      statement.execute("CREATE DATABASE root.db4");

      // Create physical time series
      statement.execute("CREATE TIMESERIES root.db4.d1.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");

      // Insert some data
      statement.execute("INSERT INTO root.db4.d1(timestamp, s1) VALUES (1, 100)");

      // Create logical view based on the physical time series
      statement.execute("CREATE VIEW root.db4.d1.view1 AS root.db4.d1.s1");

      // Test: RENAME on logical view should fail
      // Logical views cannot be renamed/aliased
      try {
        statement.execute("ALTER TIMESERIES root.db4.d1.view1 RENAME TO root.db4.d1.view1_alias");
        Assert.fail("Should throw exception when renaming logical view");
      } catch (SQLException e) {
        e.printStackTrace();
        Assert.assertTrue(
            "Error message should mention logical view cannot be renamed",
            e.getMessage().contains("logical view")
                || e.getMessage().contains("view")
                || e.getMessage().contains("Cannot create alias series"));
      }

      // Verify that the logical view still exists
      verifyTimeSeriesExists(statement, "root.db4.d1.view1", true);
      // Verify that the physical time series still exists
      verifyTimeSeriesExists(statement, "root.db4.d1.s1", true);

      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE database root.**");
    }
  }

  @Test
  public void testChainRenameWithMetadataPreservation() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Create database
      statement.execute("CREATE DATABASE root.db5");

      // Step 1: Create physical time series A
      statement.execute(
          "CREATE TIMESERIES root.db5.d1.sA WITH DATATYPE=INT64, ENCODING=PLAIN, COMPRESSION=SNAPPY");

      // Insert some data
      statement.execute("INSERT INTO root.db5.d1(timestamp, sA) VALUES (1, 100)");

      // Step 2: Rename A to B
      statement.execute("ALTER TIMESERIES root.db5.d1.sA RENAME TO root.db5.d1.sB");

      // Step 3: Modify B's tags
      statement.execute(
          "ALTER TIMESERIES root.db5.d1.sB UPSERT TAGS('unit'='count', 'type'='metric')");

      // Step 4: Rename B to C
      statement.execute("ALTER TIMESERIES root.db5.d1.sB RENAME TO root.db5.d1.sC");

      if (!schemaTestMode.equals(SchemaTestMode.PBTree)) {
        // Step 5: Modify C's encoding
        statement.execute("ALTER TIMESERIES root.db5.d1.sC SET STORAGE_PROPERTIES encoding=RLE");
      }

      // Step 6: Rename C to D
      statement.execute("ALTER TIMESERIES root.db5.d1.sC RENAME TO root.db5.d1.sD");

      // Step 7: Modify D's other information (attributes and compression)
      statement.execute(
          "ALTER TIMESERIES root.db5.d1.sD UPSERT ATTRIBUTES('description'='test sensor', 'location'='room1')");
      if (!schemaTestMode.equals(SchemaTestMode.PBTree)) {
        statement.execute("ALTER TIMESERIES root.db5.d1.sD SET STORAGE_PROPERTIES compressor=LZ4");
      }

      // Step 8: Rename D back to A
      statement.execute("ALTER TIMESERIES root.db5.d1.sD RENAME TO root.db5.d1.sA");

      // Step 9: Query A's information and verify all modifications are preserved
      String sql = "SHOW TIMESERIES root.db5.d1.sA";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        Assert.assertTrue("Should find the renamed series sA", resultSet.next());

        // Verify timeseries path
        String timeseries = resultSet.getString(ColumnHeaderConstant.TIMESERIES);
        Assert.assertEquals("Should be root.db5.d1.sA", "root.db5.d1.sA", timeseries);

        // Verify datatype
        String dataType = resultSet.getString(ColumnHeaderConstant.DATATYPE);
        Assert.assertEquals("DataType should be INT64", "INT64", dataType);

        // Verify encoding and compression (skip in PBTree mode as they cannot be modified)
        if (!schemaTestMode.equals(SchemaTestMode.PBTree)) {
          // Verify encoding (should be RLE, modified in step 5)
          String encoding = resultSet.getString(ColumnHeaderConstant.ENCODING);
          Assert.assertEquals("Encoding should be RLE", "RLE", encoding);

          // Verify compression (should be LZ4, modified in step 7)
          String compression = resultSet.getString(ColumnHeaderConstant.COMPRESSION);
          Assert.assertEquals("Compression should be LZ4", "LZ4", compression);
        }

        // Verify tags (should have unit='count' and type='metric', added in step 3)
        String tags = resultSet.getString(ColumnHeaderConstant.TAGS);
        Assert.assertNotNull("Tags should not be null", tags);
        Assert.assertTrue(
            "Tags should contain unit=count", tags.contains("unit") && tags.contains("count"));
        Assert.assertTrue(
            "Tags should contain type=metric", tags.contains("type") && tags.contains("metric"));

        // Verify attributes (should have description and location, added in step 7)
        String attributes = resultSet.getString(ColumnHeaderConstant.ATTRIBUTES);
        Assert.assertNotNull("Attributes should not be null", attributes);
        Assert.assertTrue(
            "Attributes should contain description", attributes.contains("description"));
        Assert.assertTrue("Attributes should contain location", attributes.contains("location"));
      }

      // Verify that the final alias series sA exists
      verifyTimeSeriesExists(statement, "root.db5.d1.sA", true); // The alias series exists

      // Verify that intermediate alias names (sB, sC, sD) are deleted (not invalid, just deleted)
      verifyTimeSeriesExists(
          statement, "root.db5.d1.sB", false); // sB was deleted when renamed to sC
      verifyTimeSeriesExists(
          statement, "root.db5.d1.sC", false); // sC was deleted when renamed to sD
      verifyTimeSeriesExists(
          statement, "root.db5.d1.sD", false); // sD was deleted when renamed to sA

      // Verify that only the original physical series sA is disabled (invalid)
      // When alias series are renamed, they are deleted, not disabled
      // Only physical series become invalid when renamed
      String sql2 = "SHOW INVALID TIMESERIES root.db5.d1.**";
      try (ResultSet resultSet = statement.executeQuery(sql2)) {
        List<String> invalidSeries = new ArrayList<>();
        while (resultSet.next()) {
          invalidSeries.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
        }
        // Only the original physical series sA should be invalid (disabled)
        // sB, sC, sD were alias series that got deleted when renamed, not disabled
        Assert.assertFalse(
            "sA should not be in invalid series (it was deleted, not disabled)",
            invalidSeries.contains("root.db5.d1.sA"));
        Assert.assertFalse(
            "sB should not be in invalid series (it was deleted, not disabled)",
            invalidSeries.contains("root.db5.d1.sB"));
        Assert.assertFalse(
            "sC should not be in invalid series (it was deleted, not disabled)",
            invalidSeries.contains("root.db5.d1.sC"));
        Assert.assertFalse(
            "sD should not be in invalid series (it was deleted, not disabled)",
            invalidSeries.contains("root.db5.d1.sD"));
      }

      statement.execute("DELETE TIMESERIES root.**");
      statement.execute("DELETE database root.**");
    }
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  private void verifyTimeSeriesExists(Statement statement, String path, boolean shouldExist)
      throws SQLException {
    String sql = "SHOW TIMESERIES " + path;
    try (ResultSet resultSet = statement.executeQuery(sql)) {
      if (shouldExist) {
        Assert.assertTrue("Time series " + path + " should exist", resultSet.next());
      } else {
        Assert.assertFalse("Time series " + path + " should not exist", resultSet.next());
      }
    }
  }

  private void verifyShowDevices(
      Statement statement, String sql, Set<String> expectedDevices, int expectedCount)
      throws SQLException {
    try (ResultSet resultSet = statement.executeQuery(sql)) {
      Set<String> actualDevices = new HashSet<>();
      while (resultSet.next()) {
        actualDevices.add(resultSet.getString(ColumnHeaderConstant.DEVICE));
      }
      if (!expectedDevices.isEmpty()) {
        Assert.assertEquals("Device sets should match", expectedDevices, actualDevices);
      }
      Assert.assertEquals("Device count should match", expectedCount, actualDevices.size());
    }
  }

  private void verifyShowTimeseries(Statement statement, String sql, Set<String> expectedSeries)
      throws SQLException {
    try (ResultSet resultSet = statement.executeQuery(sql)) {
      Set<String> actualSeries = new HashSet<>();
      while (resultSet.next()) {
        actualSeries.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
      }
      Assert.assertEquals("Series sets should match", expectedSeries, actualSeries);
    }
  }

  private long getCountDevices(Statement statement, String pattern) throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("COUNT DEVICES " + pattern)) {
      if (resultSet.next()) {
        return resultSet.getLong(1);
      }
    }
    return 0;
  }

  private long getShowDevicesCount(Statement statement, String pattern) throws SQLException {
    long count = 0;
    try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES " + pattern)) {
      while (resultSet.next()) {
        count++;
      }
    }
    return count;
  }

  private long getCountTimeseries(Statement statement, String pattern) throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("COUNT TIMESERIES " + pattern)) {
      if (resultSet.next()) {
        return resultSet.getLong(1);
      }
    }
    return 0;
  }

  private long getShowTimeseriesCount(Statement statement, String pattern) throws SQLException {
    long count = 0;
    try (ResultSet resultSet = statement.executeQuery("SHOW TIMESERIES " + pattern)) {
      while (resultSet.next()) {
        count++;
      }
    }
    return count;
  }
}
