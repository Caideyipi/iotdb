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
package com.timecho.iotdb;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.AdditionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.ICreateLogicalViewPlan;
import org.apache.iotdb.db.tools.schema.SRStatementGenerator;
import org.apache.iotdb.db.tools.schema.SchemaRegionSnapshotParser;

import com.timecho.iotdb.schemaregion.EnterpriseSchemaConstant;
import com.timecho.iotdb.schemaregion.mtree.EnterpriseCachedMNodeFactory;
import com.timecho.iotdb.schemaregion.mtree.EnterpriseMemMNodeFactory;
import org.apache.commons.io.FileUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class SchemaRegionSnapshotViewParserTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private SchemaRegionSnapshotParserTestParams rawConfig;

  protected final SchemaRegionSnapshotParserTestParams testParams;

  protected static class SchemaRegionSnapshotParserTestParams {
    private final String testModeName;
    private final String schemaRegionMode;

    private SchemaRegionSnapshotParserTestParams(String testModeName, String schemaEngineMode) {
      this.testModeName = testModeName;
      this.schemaRegionMode = schemaEngineMode;
    }

    public String getTestModeName() {
      return this.testModeName;
    }

    public String getSchemaRegionMode() {
      return this.schemaRegionMode;
    }

    @Override
    public String toString() {
      return testModeName;
    }
  }

  private String snapshotFileName;

  @Parameterized.Parameters(name = "{0}")
  public static List<SchemaRegionSnapshotParserTestParams> getTestModes() {
    return Arrays.asList(
        new SchemaRegionSnapshotParserTestParams("MemoryMode", "Memory"),
        new SchemaRegionSnapshotParserTestParams("PBTree", "PBTree"));
  }

  @Before
  public void setUp() {
    rawConfig =
        new SchemaRegionSnapshotParserTestParams("Raw-Config", COMMON_CONFIG.getSchemaEngineMode());
    COMMON_CONFIG.setSchemaEngineMode(testParams.schemaRegionMode);
    MNodeFactoryLoader.getInstance().addNodeFactory(EnterpriseMemMNodeFactory.class);
    MNodeFactoryLoader.getInstance().addNodeFactory(EnterpriseCachedMNodeFactory.class);
    MNodeFactoryLoader.getInstance().setEnv(EnterpriseSchemaConstant.ENTERPRISE_MNODE_FACTORY_ENV);
    SchemaEngine.getInstance().init();
    if (testParams.schemaRegionMode.equals("Memory")) {
      snapshotFileName = SchemaConstant.MTREE_SNAPSHOT;
    } else if (testParams.schemaRegionMode.equals("PBTree")) {
      snapshotFileName = SchemaConstant.PBTREE_SNAPSHOT;
    }
  }

  @After
  public void tearDown() throws Exception {
    SchemaEngine.getInstance().clear();
    cleanEnv();
    COMMON_CONFIG.setSchemaEngineMode(rawConfig.schemaRegionMode);
  }

  protected void cleanEnv() throws IOException {
    FileUtils.deleteDirectory(new File(IoTDBDescriptor.getInstance().getConfig().getSchemaDir()));
  }

  public SchemaRegionSnapshotViewParserTest(SchemaRegionSnapshotParserTestParams params) {
    this.testParams = params;
  }

  public ISchemaRegion getSchemaRegion(final String database, final int schemaRegionId)
      throws Exception {
    final SchemaRegionId regionId = new SchemaRegionId(schemaRegionId);
    if (SchemaEngine.getInstance().getSchemaRegion(regionId) == null) {
      SchemaEngine.getInstance().createSchemaRegion(database, regionId);
    }
    return SchemaEngine.getInstance().getSchemaRegion(regionId);
  }

  @Test
  public void testLogicalViewSnapshotParser() throws Exception {
    if (testParams.testModeName.equals("PBTree")) {
      return;
    }
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    PartialPath databasePath = new PartialPath("root.sg");
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.s1.g1.temp"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        0);
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.s1.g1.level"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        0);
    TimeSeriesViewOperand timeseries1 = new TimeSeriesViewOperand("root.sg.s1.g1.temp");
    TimeSeriesViewOperand timeseries2 = new TimeSeriesViewOperand("root.sg.s1.g1.level");

    AdditionViewExpression additionViewExpression =
        new AdditionViewExpression(timeseries1, timeseries2);
    ICreateLogicalViewPlan viewPlan =
        SchemaRegionWritePlanFactory.getCreateLogicalViewPlan(
            new PartialPath("root.sg.s1.g1.num"), additionViewExpression);
    schemaRegion.createLogicalView(viewPlan);

    File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
    snapshotDir.mkdir();
    schemaRegion.createSnapshot(snapshotDir);
    SRStatementGenerator statements =
        SchemaRegionSnapshotParser.translate2Statements(
            Paths.get(
                config.getSchemaDir()
                    + File.separator
                    + "snapshot"
                    + File.separator
                    + snapshotFileName),
            null,
            null,
            databasePath);

    int count = 0;
    for (Object stmt : statements) {
      if (stmt instanceof CreateLogicalViewStatement) {
        CreateLogicalViewStatement createLogicalViewStatement = (CreateLogicalViewStatement) stmt;
        Assert.assertEquals(viewPlan.getViewPathList(), createLogicalViewStatement.getPaths());
        AdditionViewExpression additionViewExpression1 =
            (AdditionViewExpression) createLogicalViewStatement.getViewExpressions().get(0);
        Assert.assertEquals(
            additionViewExpression.getLeftExpression(),
            additionViewExpression1.getLeftExpression());
        Assert.assertEquals(
            additionViewExpression.getRightExpression(),
            additionViewExpression1.getRightExpression());
      }
      count++;
    }
    Assert.assertEquals(3, count);
  }

  @Test
  public void testLogicalViewAlterSnapshotParser() throws Exception {
    if (testParams.testModeName.equals("PBTree")) {
      return;
    }
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    PartialPath databasePath = new PartialPath("root.sg");
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.s1.g1.temp"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        0);
    schemaRegion.createTimeSeries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new MeasurementPath("root.sg.s1.g1.level"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        0);
    TimeSeriesViewOperand timeseries1 = new TimeSeriesViewOperand("root.sg.s1.g1.temp");
    TimeSeriesViewOperand timeseries2 = new TimeSeriesViewOperand("root.sg.s1.g1.level");

    PartialPath path = new PartialPath("root.sg.s1.g1.num");
    AdditionViewExpression additionViewExpression =
        new AdditionViewExpression(timeseries1, timeseries2);
    ICreateLogicalViewPlan viewPlan =
        SchemaRegionWritePlanFactory.getCreateLogicalViewPlan(path, additionViewExpression);
    schemaRegion.createLogicalView(viewPlan);
    Map<String, String> tagsMap = new HashMap<>();
    Map<String, String> attributesMap = new HashMap<>();
    tagsMap.put("tag1", "t1");
    tagsMap.put("tag2", "t2");
    attributesMap.put("att1", "a1");
    attributesMap.put("att2", "a2");

    schemaRegion.upsertAliasAndTagsAndAttributes(
        null, tagsMap, attributesMap, new PartialPath("root.sg.s1.g1.num"));
    File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
    snapshotDir.mkdir();
    schemaRegion.createSnapshot(snapshotDir);
    SRStatementGenerator statements =
        SchemaRegionSnapshotParser.translate2Statements(
            Paths.get(
                config.getSchemaDir()
                    + File.separator
                    + "snapshot"
                    + File.separator
                    + snapshotFileName),
            Paths.get(
                config.getSchemaDir()
                    + File.separator
                    + "snapshot"
                    + File.separator
                    + SchemaConstant.TAG_LOG_SNAPSHOT),
            null,
            databasePath);

    int count = 0;
    boolean createdView = false;
    for (Object stmt : statements) {
      if (stmt instanceof AlterTimeSeriesStatement) {
        Assert.assertTrue(createdView);
        AlterTimeSeriesStatement alterTimeSeriesStatement = (AlterTimeSeriesStatement) stmt;
        Assert.assertEquals(path, alterTimeSeriesStatement.getPath());
        Assert.assertEquals(attributesMap, alterTimeSeriesStatement.getAttributesMap());
        Assert.assertEquals(tagsMap, alterTimeSeriesStatement.getTagsMap());
        Assert.assertEquals(
            AlterTimeSeriesStatement.AlterType.UPSERT, alterTimeSeriesStatement.getAlterType());
      } else if (stmt instanceof CreateLogicalViewStatement) {
        createdView = true;
      }
      count++;
    }
    Assert.assertEquals(4, count);
  }
}
