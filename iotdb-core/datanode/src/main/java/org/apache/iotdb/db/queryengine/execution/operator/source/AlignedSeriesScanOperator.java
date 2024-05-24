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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AlignedSeriesScanOperator extends AbstractDataSourceOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AlignedSeriesScanOperator.class);

  private final int valueColumnCount;
  private boolean finished = false;
  private int maxTsBlockLineNum = -1;

  public AlignedSeriesScanOperator(
      OperatorContext context,
      PlanNodeId sourceId,
      AlignedFullPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions,
      boolean queryAllSensors,
      List<TSDataType> dataTypes,
      int maxTsBlockLineNum) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.seriesScanUtil =
        new AlignedSeriesScanUtil(
            seriesPath,
            scanOrder,
            seriesScanOptions,
            context.getInstanceContext(),
            queryAllSensors,
            dataTypes);
    this.valueColumnCount = seriesPath.getColumnNum();
    this.maxReturnSize =
        Math.min(
            maxReturnSize,
            (1L + valueColumnCount)
                * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    this.maxTsBlockLineNum = maxTsBlockLineNum;
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    resultTsBlock = resultTsBlockBuilder.build();
    resultTsBlockBuilder.reset();
    return checkTsBlockSizeAndGetResult();
  }

  @SuppressWarnings("squid:S112")
  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }
    try {

      // start stopwatch
      long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
      long start = System.nanoTime();

      // here use do-while to promise doing this at least once
      do {
        /*
         * 1. consume page data firstly
         * 2. consume chunk data secondly
         * 3. consume next file finally
         */
        if (!readPageData() && !readChunkData() && !readFileData()) {
          break;
        }

      } while (System.nanoTime() - start < maxRuntime
          && !resultTsBlockBuilder.isFull()
          && retainedTsBlock == null);

      finished = (resultTsBlockBuilder.isEmpty() && retainedTsBlock == null);

      return !finished;
    } catch (IOException e) {
      throw new RuntimeException("Error happened while scanning the file", e);
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(
        maxReturnSize,
        (1L + valueColumnCount)
            * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte()
            * 3L);
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return calculateMaxPeekMemoryWithCounter() - calculateMaxReturnSize();
  }

  private boolean readFileData() throws IOException {
    while (seriesScanUtil.hasNextFile()) {
      if (readChunkData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readChunkData() throws IOException {
    while (seriesScanUtil.hasNextChunk()) {
      if (readPageData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readPageData() throws IOException {
    while (seriesScanUtil.hasNextPage()) {
      TsBlock tsBlock = seriesScanUtil.nextPage();
      if (!isEmpty(tsBlock)) {
        appendToBuilder(tsBlock);
        return true;
      }
    }
    return false;
  }

  private void appendToBuilder(TsBlock tsBlock) {
    int size = tsBlock.getPositionCount();
    if (resultTsBlockBuilder.isEmpty()
        && tsBlock.getPositionCount() >= resultTsBlockBuilder.getMaxTsBlockLineNumber()) {
      retainedTsBlock = tsBlock;
      return;
    }
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    for (int i = 0; i < size; i++) {
      timeColumnBuilder.writeLong(timeColumn.getLong(i));
      resultTsBlockBuilder.declarePosition();
    }
    for (int columnIndex = 0, columnSize = tsBlock.getValueColumnCount();
        columnIndex < columnSize;
        columnIndex++) {
      appendOneColumn(columnIndex, tsBlock, size);
    }
  }

  private void appendOneColumn(int columnIndex, TsBlock tsBlock, int size) {
    ColumnBuilder columnBuilder = resultTsBlockBuilder.getColumnBuilder(columnIndex);
    Column column = tsBlock.getColumn(columnIndex);
    if (column.mayHaveNull()) {
      for (int i = 0; i < size; i++) {
        if (column.isNull(i)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.write(column, i);
        }
      }
    } else {
      for (int i = 0; i < size; i++) {
        columnBuilder.write(column, i);
      }
    }
  }

  private boolean isEmpty(TsBlock tsBlock) {
    return tsBlock == null || tsBlock.isEmpty();
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    // time + all value columns
    return seriesScanUtil.getTsDataTypeList();
  }

  @Override
  public void initQueryDataSource(IQueryDataSource dataSource) {
    seriesScanUtil.initQueryDataSource((QueryDataSource) dataSource);
    resultTsBlockBuilder = new TsBlockBuilder(getResultDataTypes());
    resultTsBlockBuilder.setMaxTsBlockLineNumber(this.maxTsBlockLineNum);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(seriesScanUtil)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId)
        + (resultTsBlockBuilder == null ? 0 : resultTsBlockBuilder.getRetainedSizeInBytes());
  }
}
