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

package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.operator.process.FilterAndProjectOperator.satisfy;
import static org.apache.iotdb.db.queryengine.execution.operator.schema.source.TableDeviceQuerySource.transformToTsBlockColumns;

public abstract class DevicePredicateHandler implements AutoCloseable {
  protected final List<LeafColumnTransformer> filterLeafColumnTransformerList;
  protected final ColumnTransformer filterOutputTransformer;
  protected final List<ColumnTransformer> commonTransformerList;
  protected final List<TSDataType> filterOutputDataTypes;
  protected final List<TSDataType> inputDataTypes;
  protected final String database;
  protected final String tableName;
  protected final List<ColumnHeader> columnHeaderList;

  // Batch logic
  private static final int DEFAULT_MAX_TS_BLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  protected final List<IDeviceSchemaInfo> deviceSchemaBatch =
      new ArrayList<>(DEFAULT_MAX_TS_BLOCK_SIZE_IN_BYTES);
  protected final TsBlockBuilder filterTsBlockBuilder;
  protected final List<Integer> indexes = new ArrayList<>();
  protected TsBlock curBlock;
  protected Column curFilterColumn;

  protected DevicePredicateHandler(
      final List<TSDataType> filterOutputDataTypes,
      final List<LeafColumnTransformer> filterLeafColumnTransformerList,
      final ColumnTransformer filterOutputTransformer,
      final List<ColumnTransformer> commonTransformerList,
      final String database,
      final String tableName,
      final List<ColumnHeader> columnHeaderList) {
    this.filterOutputDataTypes = filterOutputDataTypes;
    this.filterLeafColumnTransformerList = filterLeafColumnTransformerList;
    this.filterOutputTransformer = filterOutputTransformer;
    this.commonTransformerList = commonTransformerList;
    this.database = database;
    this.tableName = tableName;
    this.columnHeaderList = columnHeaderList;
    this.inputDataTypes =
        columnHeaderList.stream().map(ColumnHeader::getColumnType).collect(Collectors.toList());
    this.filterTsBlockBuilder = new TsBlockBuilder(filterOutputDataTypes);
  }

  public boolean addBatch(final IDeviceSchemaInfo deviceSchemaInfo) {
    deviceSchemaBatch.add(deviceSchemaInfo);
    final boolean result = deviceSchemaBatch.size() >= DEFAULT_MAX_TS_BLOCK_SIZE_IN_BYTES;
    if (result) {
      prepareBatchResult();
    }
    return result;
  }

  protected void clear() {
    curBlock = null;
    curFilterColumn = null;
    indexes.clear();
    deviceSchemaBatch.clear();
  }

  public void prepareBatchResult() {
    if (deviceSchemaBatch.isEmpty()) {
      return;
    }
    final TsBlockBuilder builder = new TsBlockBuilder(inputDataTypes);
    deviceSchemaBatch.forEach(
        deviceSchemaInfo ->
            transformToTsBlockColumns(
                deviceSchemaInfo, builder, database, tableName, columnHeaderList, 3));

    curBlock = builder.build();
    if (Objects.isNull(filterOutputTransformer)) {
      return;
    }

    // feed Filter ColumnTransformer, including TimeStampColumnTransformer and constant
    filterLeafColumnTransformerList.forEach(
        leafColumnTransformer -> leafColumnTransformer.initFromTsBlock(curBlock));
    filterOutputTransformer.tryEvaluate();
    final Column filterColumn = filterOutputTransformer.getColumn();

    for (int j = 0; j < deviceSchemaBatch.size(); j++) {
      if (satisfy(filterColumn, j)) {
        indexes.add(j);
      }
    }
    curFilterColumn = filterColumn;
  }

  @Override
  public void close() {
    clear();
    if (Objects.nonNull(filterOutputTransformer)) {
      filterOutputTransformer.close();
    }
  }
}
