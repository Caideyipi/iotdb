/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.IdColumnSchema;
import org.apache.iotdb.commons.schema.table.column.MeasurementColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableDeviceSchemaCacheTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private long originMemConfig;

  @Before
  public void setup() {
    originMemConfig = config.getAllocateMemoryForSchemaCache();
    config.setAllocateMemoryForSchemaCache(1500L);
  }

  @After
  public void rollback() {
    config.setAllocateMemoryForSchemaCache(originMemConfig);
  }

  @Test
  public void testDeviceCache() {
    final TableDeviceSchemaCache cache = new TableDeviceSchemaCache();

    final String database = "db";
    final String table1 = "t1";

    final Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("type", "new");
    attributeMap.put("cycle", "monthly");
    cache.putAttributes(
        database,
        table1,
        new String[] {"hebei", "p_1", "d_0"},
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(database, table1, new String[] {"hebei", "p_1", "d_0"}));
    Assert.assertNull(
        cache.getDeviceAttribute(database, table1, new String[] {"hebei", "p_1", "d_1"}));

    attributeMap.put("type", "old");
    cache.putAttributes(
        database,
        table1,
        new String[] {"hebei", "p_1", "d_1"},
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(database, table1, new String[] {"hebei", "p_1", "d_1"}));

    attributeMap.put("cycle", "daily");
    cache.putAttributes(
        database,
        table1,
        new String[] {"shandong", "p_1", "d_1"},
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertNull(
        cache.getDeviceAttribute(database, table1, new String[] {"hebei", "p_1", "d_0"}));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(database, table1, new String[] {"shandong", "p_1", "d_1"}));

    final String table2 = "t1";
    attributeMap.put("type", "new");
    attributeMap.put("cycle", "monthly");
    cache.putAttributes(
        database,
        table2,
        new String[] {"hebei", "p_1", "d_0"},
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(database, table2, new String[] {"hebei", "p_1", "d_0"}));
    Assert.assertNull(
        cache.getDeviceAttribute(database, table1, new String[] {"hebei", "p_1", "d_1"}));

    attributeMap.put("type", "old");
    cache.putAttributes(
        database,
        table2,
        new String[] {"hebei", "p_1", "d_1"},
        new ConcurrentHashMap<>(attributeMap));
    Assert.assertEquals(
        attributeMap,
        cache.getDeviceAttribute(database, table2, new String[] {"hebei", "p_1", "d_1"}));
    Assert.assertNull(
        cache.getDeviceAttribute(database, table1, new String[] {"shandong", "p_1", "d_1"}));
  }

  @Test
  public void testLastCache() {
    final TableDeviceSchemaCache cache = new TableDeviceSchemaCache();

    final String database = "db";
    final String table1 = "t1";

    final String[] device0 = new String[] {"hebei", "p_1", "d_0"};

    // Query update
    final Map<String, TimeValuePair> measurementQueryUpdateMap = new HashMap<>();

    final TimeValuePair tv1 = new TimeValuePair(0L, new TsPrimitiveType.TsInt(1));
    final TimeValuePair tv2 = new TimeValuePair(1L, new TsPrimitiveType.TsInt(2));
    measurementQueryUpdateMap.put("s1", tv1);
    measurementQueryUpdateMap.put("s2", tv2);

    cache.updateLastCache(database, table1, device0, measurementQueryUpdateMap);

    Assert.assertEquals(tv1, cache.getLastEntry(database, table1, device0, "s1"));
    Assert.assertEquals(tv2, cache.getLastEntry(database, table1, device0, "s2"));

    // Write update existing
    final Map<String, TimeValuePair> measurementWriteUpdateMap = new HashMap<>();

    final TimeValuePair tv3 = new TimeValuePair(0L, new TsPrimitiveType.TsInt(1));
    final TimeValuePair tv4 = new TimeValuePair(1L, new TsPrimitiveType.TsInt(2));
    measurementWriteUpdateMap.put("s2", tv3);
    measurementWriteUpdateMap.put("s3", tv4);

    cache.tryUpdateLastCacheWithoutLock(database, table1, device0, measurementWriteUpdateMap);

    Assert.assertEquals(tv3, cache.getLastEntry(database, table1, device0, "s2"));
    Assert.assertEquals(tv4, cache.getLastEntry(database, table1, device0, "s3"));

    // Write update non-exist
    final String database2 = "db2";
    final String table2 = "t2";

    cache.tryUpdateLastCacheWithoutLock(database, table2, device0, measurementWriteUpdateMap);
    cache.tryUpdateLastCacheWithoutLock(database2, table1, device0, measurementWriteUpdateMap);

    Assert.assertNull(cache.getLastEntry(database, table2, device0, "s2"));
    Assert.assertNull(cache.getLastEntry(database2, table1, device0, "s2"));

    // Invalidate device
    cache.invalidateLastCache(database, table1, device0);
    Assert.assertNull(cache.getLastEntry(database, table1, device0, "s2"));

    // Invalidate table
    final String[] device1 = new String[] {"hebei", "p_1", "d_1"};

    cache.updateLastCache(database, table2, device0, measurementQueryUpdateMap);
    cache.updateLastCache(database, table2, device1, measurementQueryUpdateMap);

    cache.invalidateLastCache(database, table2, null);

    Assert.assertNull(cache.getLastEntry(database, table2, device0, "s2"));
    Assert.assertNull(cache.getLastEntry(database, table2, device1, "s2"));
  }

  @Test
  public void testIntern() {
    final String database = "sg";
    final String tableName = "t";
    final List<ColumnHeader> columnHeaderList =
        Arrays.asList(
            new ColumnHeader("hebei", TSDataType.STRING),
            new ColumnHeader("p_1", TSDataType.STRING),
            new ColumnHeader("d_1", TSDataType.STRING));
    final String attributeName = "attr";

    // Prepare table
    final TsTable testTable = new TsTable(tableName);
    columnHeaderList.forEach(
        columnHeader ->
            testTable.addColumnSchema(
                new IdColumnSchema(columnHeader.getColumnName(), columnHeader.getColumnType())));
    testTable.addColumnSchema(new AttributeColumnSchema(attributeName, TSDataType.STRING));
    testTable.addColumnSchema(new TimeColumnSchema("time", TSDataType.INT64));
    testTable.addColumnSchema(
        new MeasurementColumnSchema(
            "s1", TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.GZIP));
    DataNodeTableCache.getInstance().preUpdateTable(database, testTable);
    DataNodeTableCache.getInstance().commitUpdateTable(database, tableName);

    final String a = "s1";
    // Different from "a"
    final String b = new String(a.getBytes());

    Assert.assertSame(
        DataNodeTableCache.getInstance().tryGetInternColumnName(database, tableName, a),
        DataNodeTableCache.getInstance().tryGetInternColumnName(database, tableName, b));
  }
}
