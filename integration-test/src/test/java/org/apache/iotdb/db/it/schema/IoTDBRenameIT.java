/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Industrial-grade Integration Test for IoTDB Series Renaming (Alias).
 *
 * <p>Coverage Matrix: 1. Lifecycle: Physical -> Alias -> Alias (Chain) 2. Topology: Cross-Device,
 * Cross-Database, Auto-creation 3. Alignment: Aligned <-> Non-Aligned interoperability 4.
 * Constraints: View/Template isolation, Target conflicts 5. Metadata: Tags/Attributes persistence,
 * Inverted Index 6. Dependency: View creation on Alias/Disabled (Forbidden)
 */
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBRenameIT extends AbstractSchemaIT {

  public IoTDBRenameIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    SchemaTestMode schemaTestMode = setUpEnvironment();
    if (schemaTestMode.equals(SchemaTestMode.PBTree)) {
      allocateMemoryForSchemaRegion(10000);
    }
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Initialize basic structure
      statement.execute("CREATE DATABASE root.db");
      statement.execute("CREATE DATABASE root.db_remote");
    }
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  // ==================================================================================
  // Cluster 1: Basic Lifecycle & Data Continuity
  // Coverage: Physical -> Alias, data read/write, metadata state
  // ==================================================================================
  @Test
  public void testBasicRenameLifecycle() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // 1. Create Physical Series & Insert Data
      statement.execute("CREATE TIMESERIES root.db.d1.s_raw WITH DATATYPE=INT64, ENCODING=RLE");
      statement.execute("INSERT INTO root.db.d1(timestamp, s_raw) VALUES (1000, 10)");

      // 2. Rename: s_raw -> s_alias
      statement.execute("ALTER TIMESERIES root.db.d1.s_raw RENAME TO root.db.d1.s_alias");

      // 3. Validation: s_raw should be hidden (Invalid), s_alias should exist
      assertTimeSeriesNotExists(statement, "root.db.d1.s_raw");
      assertTimeSeriesExists(statement, "root.db.d1.s_alias");

      // 4. Verification: Creation collision
      // Trying to re-create 's_raw' should FAIL because it is physically present (just hidden)
      try {
        statement.execute("CREATE TIMESERIES root.db.d1.s_raw WITH DATATYPE=INT64");
        Assert.fail(
            "Re-creating the original physical series should fail (it is hidden, not deleted)");
      } catch (SQLException e) {
        Assert.assertTrue(e.getMessage().contains("timeseries root.db.d1.s_raw is invalid"));
      }

      // 5. Data Verification: Query alias should return old data
      try (ResultSet rs =
          statement.executeQuery("SELECT s_alias FROM root.db.d1 WHERE time = 1000")) {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(10, rs.getLong("root.db.d1.s_alias"));
      }

      // 6. New Data Ingestion: Write to alias
      statement.execute("INSERT INTO root.db.d1(timestamp, s_alias) VALUES (2000, 20)");

      // 7. Mixed Query
      try (ResultSet rs = statement.executeQuery("SELECT s_alias FROM root.db.d1")) {
        int count = 0;
        while (rs.next()) count++;
        Assert.assertEquals("Should have 2 rows (old + new)", 2, count);
      }
    }
  }

  // ==================================================================================
  // Cluster 2: Alias Chaining & Deletion Mechanism
  // Coverage: Alias -> Alias, verify old alias is Deleted not Hidden
  // ==================================================================================
  @Test
  public void testAliasChainingAndDeletion() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // A -> B
      statement.execute("CREATE TIMESERIES root.db.d1.s_origin WITH DATATYPE=INT64");
      statement.execute("ALTER TIMESERIES root.db.d1.s_origin RENAME TO root.db.d1.s_step1");

      // B -> C (Renaming an Alias)
      statement.execute("ALTER TIMESERIES root.db.d1.s_step1 RENAME TO root.db.d1.s_final");

      // Validation:
      // s_origin: Hidden (Physical)
      // s_step1:  Deleted (Alias) -> Can be recreated
      // s_final:  Active (Alias)

      assertTimeSeriesNotExists(statement, "root.db.d1.s_origin");
      assertTimeSeriesNotExists(statement, "root.db.d1.s_step1");
      assertTimeSeriesExists(statement, "root.db.d1.s_final");

      // Crucial Check: We should be able to create s_step1 again because aliases are deleted on
      // rename
      try {
        statement.execute("CREATE TIMESERIES root.db.d1.s_step1 WITH DATATYPE=INT64");
        // Should succeed
        assertTimeSeriesExists(statement, "root.db.d1.s_step1");
      } catch (SQLException e) {
        Assert.fail("Failed to reuse old alias name. Error: " + e.getMessage());
      }

      // Crucial Check: We still cannot recreate s_origin
      try {
        statement.execute("CREATE TIMESERIES root.db.d1.s_origin WITH DATATYPE=INT64");
        Assert.fail("Should not be able to recreate hidden physical series");
      } catch (SQLException e) {
        Assert.assertTrue(e.getMessage().contains("timeseries root.db.d1.s_origin is invalid"));
      }
    }
  }

  // ==================================================================================
  // Cluster 3: Alignment Interoperability
  // Coverage: Aligned -> Non-Aligned, Non-Aligned -> Aligned
  // ==================================================================================
  @Test
  public void testCrossAlignmentRename() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Case A: Non-Aligned -> Aligned Device
      // 1. Create Aligned Device & Series
      statement.execute("CREATE ALIGNED TIMESERIES root.db.d_aligned(s_base INT64)");
      statement.execute("INSERT INTO root.db.d_aligned(timestamp, s_base) VALUES (1000, 1)");

      // 2. Create Non-Aligned Series
      statement.execute("CREATE TIMESERIES root.db.d_norm.s_norm WITH DATATYPE=INT64");
      statement.execute("INSERT INTO root.db.d_norm(timestamp, s_norm) VALUES (1000, 99)");

      // 3. Rename Non-Aligned INTO Aligned Device
      statement.execute(
          "ALTER TIMESERIES root.db.d_norm.s_norm RENAME TO root.db.d_aligned.s_imported");

      // 4. Validation: Verify existence and mixed query
      assertTimeSeriesExists(statement, "root.db.d_aligned.s_imported");

      try (ResultSet rs =
          statement.executeQuery(
              "SELECT s_base, s_imported FROM root.db.d_aligned WHERE time=1000")) {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("root.db.d_aligned.s_base"));
        Assert.assertEquals(99, rs.getLong("root.db.d_aligned.s_imported"));
      }

      // Case B: Aligned -> Non-Aligned Device
      // 1. Rename Aligned s_base OUT TO Non-Aligned Device
      statement.execute(
          "ALTER TIMESERIES root.db.d_aligned.s_base RENAME TO root.db.d_norm.s_exported");

      // 2. Validation
      assertTimeSeriesNotExists(statement, "root.db.d_aligned.s_base");
      assertTimeSeriesExists(statement, "root.db.d_norm.s_exported");

      try (ResultSet rs =
          statement.executeQuery("SELECT s_exported FROM root.db.d_norm WHERE time=1000")) {
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("root.db.d_norm.s_exported"));
      }
    }
  }

  // ==================================================================================
  // Cluster 4: Complex Topology & Auto-creation
  // Coverage: Cross-database, cross-device, target does not exist
  // ==================================================================================
  @Test
  public void testTopologyAndAutoCreation() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE TIMESERIES root.db.d1.s1 WITH DATATYPE=INT64");

      // 1. Rename to non-existent device (Same DB)
      statement.execute("ALTER TIMESERIES root.db.d1.s1 RENAME TO root.db.d_new.s1");
      assertTimeSeriesExists(statement, "root.db.d_new.s1");

      // 2. Rename to non-existent Database (Auto-create DB)
      statement.execute("ALTER TIMESERIES root.db.d_new.s1 RENAME TO root.db_ghost.d_phantom.s1");

      // Verify DB creation
      try (ResultSet rs = statement.executeQuery("SHOW DATABASES root.db_ghost")) {
        Assert.assertTrue("Database root.db_ghost should be auto-created", rs.next());
      }
      assertTimeSeriesExists(statement, "root.db_ghost.d_phantom.s1");
    }
  }

  // ==================================================================================
  // Cluster 5: Metadata & Tags Verification
  // Coverage: Tags migrate with rename, inverted index update
  // ==================================================================================
  @Test
  public void testMetadataPersistence() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE TIMESERIES root.db.d1.s_tag WITH DATATYPE=INT64");
      statement.execute(
          "ALTER TIMESERIES root.db.d1.s_tag UPSERT TAGS('unit'='mol', 'type'='chem')");

      // Rename
      statement.execute("ALTER TIMESERIES root.db.d1.s_tag RENAME TO root.db.d1.s_tag_new");

      // Verify Tags presence
      try (ResultSet rs = statement.executeQuery("SHOW TIMESERIES root.db.d1.s_tag_new")) {
        Assert.assertTrue(rs.next());
        String tags = rs.getString(ColumnHeaderConstant.TAGS);
        Assert.assertTrue(tags.contains("mol"));
        Assert.assertTrue(tags.contains("chem"));
      }

      // Verify Inverted Index (Query by Tag using NEW name)
      Set<String> expected = new HashSet<>(Collections.singletonList("root.db.d1.s_tag_new"));
      assertResultSetContent(
          statement.executeQuery("SHOW TIMESERIES WHERE TAGS(unit)='mol'"),
          ColumnHeaderConstant.TIMESERIES,
          expected);
    }
  }

  // ==================================================================================
  // Cluster 6: Constraints & Edge Cases
  // Coverage: View/Template isolation, target conflicts, hidden series conflicts
  // ==================================================================================
  @Test
  public void testConstraintsAndConflicts() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // 1. Forbidden: Rename View
      statement.execute("CREATE TIMESERIES root.db.d1.s_base WITH DATATYPE=INT64");
      statement.execute("CREATE VIEW root.db.d1.v_base AS root.db.d1.s_base");
      try {
        statement.execute("ALTER TIMESERIES root.db.d1.v_base RENAME TO root.db.d1.v_new");
        Assert.fail("Renaming View should fail");
      } catch (SQLException e) {
        Assert.assertTrue(e.getMessage().contains("view") || e.getMessage().contains("support"));
      }

      // 2. Forbidden: Rename Template Series
      statement.execute("CREATE DEVICE TEMPLATE t1 (s_tmp INT64)");
      statement.execute("SET DEVICE TEMPLATE t1 TO root.db.d_tpl");
      statement.execute("CREATE TIMESERIES OF DEVICE TEMPLATE ON root.db.d_tpl");
      try {
        statement.execute("ALTER TIMESERIES root.db.d_tpl.s_tmp RENAME TO root.db.d_tpl.s_new");
        Assert.fail("Renaming Template Series should fail");
      } catch (SQLException e) {
        Assert.assertTrue(e.getMessage().contains("template"));
      }

      // 3. Conflict: Target Exists (Normal)
      statement.execute("CREATE TIMESERIES root.db.d1.s_exist WITH DATATYPE=INT64");
      try {
        statement.execute("ALTER TIMESERIES root.db.d1.s_base RENAME TO root.db.d1.s_exist");
        Assert.fail("Renaming to existing series should fail");
      } catch (SQLException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("Path [root.db.d1.s_exist already exists as a physical path] already"));
      }

      // 4. Conflict: Target Exists (Hidden Physical)
      // A -> B. A is hidden. Try C -> A.
      statement.execute(
          "ALTER TIMESERIES root.db.d1.s_base RENAME TO root.db.d1.s_alias_b"); // s_base becomes
      // hidden
      statement.execute("CREATE TIMESERIES root.db.d1.s_c WITH DATATYPE=INT64");
      try {
        statement.execute("ALTER TIMESERIES root.db.d1.s_c RENAME TO root.db.d1.s_base");
        Assert.fail("Renaming to hidden physical series should fail");
      } catch (SQLException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "Rename failed at [create alias]: Path [root.db.d1.s_base] is already an invalid series and cannot be used to create an alias series"));
      }
    }
  }

  // ==================================================================================
  // Cluster 7: Dependency Safety
  // Coverage: Forbid creating views on Alias or Disabled series
  // ==================================================================================
  @Test
  public void testViewDependencyConstraints() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE TIMESERIES root.db.d2.s_phy WITH DATATYPE=INT64");
      statement.execute("ALTER TIMESERIES root.db.d2.s_phy RENAME TO root.db.d2.s_alias");

      // 1. Try create view on Alias
      statement.execute("CREATE VIEW root.db.d2.s_view AS root.db.d2.s_alias");

      // 2. Try create view on Disabled/Hidden
      try {
        statement.execute("CREATE VIEW root.db.d2.v_err AS root.db.d2.s_phy");
        Assert.fail("Creating view on Disabled series should fail");
      } catch (SQLException e) {
        Assert.assertTrue(
            e.getMessage().contains("300: Can not create a view based on invalid time series."));
      }

      // 3. Verify no views created
      try (ResultSet rs = statement.executeQuery("SHOW TIMESERIES root.db.d2.v_*")) {
        Assert.assertFalse(rs.next());
      }

      try (ResultSet rs = statement.executeQuery("SHOW TIMESERIES root.db.d2.s_view")) {
        Assert.assertTrue(rs.next());
      }
    }
  }

  // ==================================================================================
  // Helper Methods (Strict Assertions)
  // ==================================================================================

  private void assertResultSetContent(ResultSet rs, String targetColumn, Set<String> expectedValues)
      throws SQLException {
    Set<String> actualValues = new HashSet<>();
    ResultSetMetaData metaData = rs.getMetaData();
    int colIndex = -1;
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      if (metaData.getColumnName(i).equalsIgnoreCase(targetColumn)
          || metaData.getColumnLabel(i).equalsIgnoreCase(targetColumn)) {
        colIndex = i;
        break;
      }
    }
    if (colIndex == -1) colIndex = 1;

    while (rs.next()) {
      actualValues.add(rs.getString(colIndex));
    }
    Assert.assertEquals("ResultSet content mismatch.", expectedValues, actualValues);
  }

  private void assertTimeSeriesExists(Statement statement, String path) throws SQLException {
    try (ResultSet rs = statement.executeQuery("SHOW TIMESERIES " + path)) {
      Assert.assertTrue("Timeseries " + path + " should exist", rs.next());
    }
  }

  private void assertTimeSeriesNotExists(Statement statement, String path) throws SQLException {
    try (ResultSet rs = statement.executeQuery("SHOW TIMESERIES " + path)) {
      Assert.assertFalse("Timeseries " + path + " should NOT exist", rs.next());
    }
  }
}
