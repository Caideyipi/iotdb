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

package org.apache.iotdb.db.pipe.event.common.tablet.parser;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class TabletInsertionEventParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(TabletInsertionEventParser.class);

  private static final LocalDate EMPTY_LOCALDATE = LocalDate.of(1000, 1, 1);

  protected final PipeTaskMeta pipeTaskMeta; // used to report progress
  protected final EnrichedEvent
      sourceEvent; // used to report progress and filter value columns by time range

  protected IDeviceID deviceId;
  protected String deviceIdString; // Used to preserve performance
  protected boolean isAligned;
  protected IMeasurementSchema[] measurementSchemaList;
  protected String[] columnNameStringList;

  protected long[] timestampColumn;
  protected ColumnCategory[] valueColumnTypes;
  protected TSDataType[] valueColumnDataTypes;
  // Each column of Object[] is a column of primitive type array
  protected Object[] valueColumns;
  protected BitMap[] nullValueColumnBitmaps;
  protected int rowCount;

  protected Tablet tablet;

  // Whether the parser shall report progress
  protected boolean shouldReport = false;

  protected static final Integer CACHED_FULL_ROW_INDEX_LIST_ROW_COUNT_UPPER = 16;
  protected static final Map<Integer, List<Integer>> CACHED_FULL_ROW_INDEX_LIST = new HashMap<>();

  static {
    for (int rowCount = 0; rowCount <= CACHED_FULL_ROW_INDEX_LIST_ROW_COUNT_UPPER; ++rowCount) {
      CACHED_FULL_ROW_INDEX_LIST.put(
          rowCount, IntStream.range(0, rowCount).boxed().collect(Collectors.toList()));
    }
  }

  protected TabletInsertionEventParser(
      final PipeTaskMeta pipeTaskMeta, final EnrichedEvent sourceEvent) {
    this.pipeTaskMeta = pipeTaskMeta;
    this.sourceEvent = sourceEvent;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void markAsNeedToReport() {
    shouldReport = true;
  }

  protected abstract Object getPattern();

  //////////////////////////// parse ////////////////////////////

  protected void parse(final InsertRowNode insertRowNode) {
    final int originColumnSize = insertRowNode.getMeasurements().length;
    final Integer[] originColumnIndex2FilteredColumnIndexMapperList = new Integer[originColumnSize];

    // The full path is always cached when device path is deserialized
    this.deviceIdString = insertRowNode.getTargetPath().getFullPath();
    this.deviceId = insertRowNode.getDeviceID();
    this.isAligned = insertRowNode.isAligned();

    final long[] originTimestampColumn = new long[] {insertRowNode.getTime()};
    final List<Integer> rowIndexList = generateRowIndexList(originTimestampColumn);
    this.timestampColumn = rowIndexList.stream().mapToLong(i -> originTimestampColumn[i]).toArray();

    generateColumnIndexMapper(
        insertRowNode.getMeasurements(), originColumnIndex2FilteredColumnIndexMapperList);

    final int filteredColumnSize =
        Arrays.stream(originColumnIndex2FilteredColumnIndexMapperList)
            .filter(Objects::nonNull)
            .toArray()
            .length;

    this.measurementSchemaList = new MeasurementSchema[filteredColumnSize];
    this.columnNameStringList = new String[filteredColumnSize];
    this.valueColumnDataTypes = new TSDataType[filteredColumnSize];
    this.valueColumnTypes = new ColumnCategory[filteredColumnSize];
    this.valueColumns = new Object[filteredColumnSize];
    this.nullValueColumnBitmaps = new BitMap[filteredColumnSize];

    final MeasurementSchema[] originMeasurementSchemaList = insertRowNode.getMeasurementSchemas();
    final String[] originColumnNameStringList = insertRowNode.getMeasurements();
    final TsTableColumnCategory[] originColumnCategories = insertRowNode.getColumnCategories();
    final TSDataType[] originValueDataTypes = insertRowNode.getDataTypes();
    final Object[] originValues = insertRowNode.getValues();

    for (int i = 0; i < originColumnIndex2FilteredColumnIndexMapperList.length; i++) {
      if (originColumnIndex2FilteredColumnIndexMapperList[i] != null) {
        final int filteredColumnIndex = originColumnIndex2FilteredColumnIndexMapperList[i];
        this.measurementSchemaList[filteredColumnIndex] = originMeasurementSchemaList[i];
        this.columnNameStringList[filteredColumnIndex] = originColumnNameStringList[i];
        this.valueColumnTypes[filteredColumnIndex] =
            originColumnCategories != null && originColumnCategories[i] != null
                ? originColumnCategories[i].toTsFileColumnType()
                : ColumnCategory.FIELD;
        this.valueColumnDataTypes[filteredColumnIndex] = originValueDataTypes[i];
        final BitMap bitMap = new BitMap(this.timestampColumn.length);
        if (Objects.isNull(originValues[i]) || Objects.isNull(originValueDataTypes[i])) {
          fillNullValue(
              originValueDataTypes[i],
              this.valueColumns,
              bitMap,
              filteredColumnIndex,
              rowIndexList.size());
        } else {
          this.valueColumns[filteredColumnIndex] =
              filterValueColumnsByRowIndexList(
                  originValueDataTypes[i],
                  originValues[i],
                  rowIndexList,
                  true,
                  bitMap, // use the output bitmap since there is no bitmap in InsertRowNode
                  bitMap);
        }
        this.nullValueColumnBitmaps[filteredColumnIndex] = bitMap;
      }
    }

    this.rowCount = this.timestampColumn.length;
    if (this.rowCount == 0 && LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "InsertRowNode({}) is parsed to zero rows according to the pattern({}) and time range [{}, {}], the corresponding source event({}) will be ignored.",
          insertRowNode,
          getPattern(),
          this.sourceEvent.getStartTime(),
          this.sourceEvent.getEndTime(),
          this.sourceEvent);
    }
  }

  protected void parse(final InsertTabletNode insertTabletNode) {
    final int originColumnSize = insertTabletNode.getMeasurements().length;
    final Integer[] originColumnIndex2FilteredColumnIndexMapperList = new Integer[originColumnSize];

    // The full path is always cached when device path is deserialized
    this.deviceIdString = insertTabletNode.getTargetPath().getFullPath();
    this.deviceId = insertTabletNode.getDeviceID();
    this.isAligned = insertTabletNode.isAligned();

    final long[] originTimestampColumn = insertTabletNode.getTimes();
    final int originRowSize = originTimestampColumn.length;
    final List<Integer> rowIndexList = generateRowIndexList(originTimestampColumn);
    this.timestampColumn = rowIndexList.stream().mapToLong(i -> originTimestampColumn[i]).toArray();

    generateColumnIndexMapper(
        insertTabletNode.getMeasurements(), originColumnIndex2FilteredColumnIndexMapperList);

    final int filteredColumnSize =
        Arrays.stream(originColumnIndex2FilteredColumnIndexMapperList)
            .filter(Objects::nonNull)
            .toArray()
            .length;

    this.measurementSchemaList = new MeasurementSchema[filteredColumnSize];
    this.columnNameStringList = new String[filteredColumnSize];
    this.valueColumnTypes = new ColumnCategory[filteredColumnSize];
    this.valueColumnDataTypes = new TSDataType[filteredColumnSize];
    this.valueColumns = new Object[filteredColumnSize];
    this.nullValueColumnBitmaps = new BitMap[filteredColumnSize];

    final MeasurementSchema[] originMeasurementSchemaList =
        insertTabletNode.getMeasurementSchemas();
    final String[] originColumnNameStringList = insertTabletNode.getMeasurements();
    final TsTableColumnCategory[] originColumnCategories = insertTabletNode.getColumnCategories();
    final TSDataType[] originValueColumnDataTypes = insertTabletNode.getDataTypes();
    final Object[] originValueColumns = insertTabletNode.getColumns();
    final BitMap[] originBitMapList =
        (insertTabletNode.getBitMaps() == null
            ? IntStream.range(0, originColumnSize)
                .boxed()
                .map(o -> new BitMap(originRowSize))
                .toArray(BitMap[]::new)
            : insertTabletNode.getBitMaps());
    for (int i = 0; i < originBitMapList.length; i++) {
      if (originBitMapList[i] == null) {
        originBitMapList[i] = new BitMap(originRowSize);
      }
    }

    for (int i = 0; i < originColumnIndex2FilteredColumnIndexMapperList.length; i++) {
      if (originColumnIndex2FilteredColumnIndexMapperList[i] != null) {
        final int filteredColumnIndex = originColumnIndex2FilteredColumnIndexMapperList[i];
        this.measurementSchemaList[filteredColumnIndex] = originMeasurementSchemaList[i];
        this.columnNameStringList[filteredColumnIndex] = originColumnNameStringList[i];
        this.valueColumnTypes[filteredColumnIndex] =
            originColumnCategories != null && originColumnCategories[i] != null
                ? originColumnCategories[i].toTsFileColumnType()
                : ColumnCategory.FIELD;
        this.valueColumnDataTypes[filteredColumnIndex] = originValueColumnDataTypes[i];
        final BitMap bitMap = new BitMap(this.timestampColumn.length);
        if (Objects.isNull(originValueColumns[i])
            || Objects.isNull(originValueColumnDataTypes[i])) {
          fillNullValue(
              originValueColumnDataTypes[i],
              this.valueColumns,
              bitMap,
              filteredColumnIndex,
              rowIndexList.size());
        } else {
          this.valueColumns[filteredColumnIndex] =
              filterValueColumnsByRowIndexList(
                  originValueColumnDataTypes[i],
                  originValueColumns[i],
                  rowIndexList,
                  false,
                  originBitMapList[i],
                  bitMap);
        }
        this.nullValueColumnBitmaps[filteredColumnIndex] = bitMap;
      }
    }

    this.rowCount = this.timestampColumn.length;
    if (rowCount == 0 && LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "InsertTabletNode({}) is parsed to zero rows according to the pattern({}) and time range [{}, {}], the corresponding source event({}) will be ignored.",
          insertTabletNode,
          getPattern(),
          sourceEvent.getStartTime(),
          sourceEvent.getEndTime(),
          sourceEvent);
    }
  }

  protected void parse(final Tablet tablet, final boolean isAligned) {
    final int originColumnSize = tablet.getSchemas().size();
    final Integer[] originColumnIndex2FilteredColumnIndexMapperList = new Integer[originColumnSize];

    // Only support tree-model tablet
    this.deviceIdString = tablet.getDeviceId();
    this.deviceId = new StringArrayDeviceID(tablet.getDeviceId());
    this.isAligned = isAligned;

    final long[] originTimestampColumn =
        Arrays.copyOf(
            tablet.getTimestamps(),
            tablet.getRowSize()); // tablet.timestamps.length == tablet.maxRowNumber
    final List<Integer> rowIndexList = generateRowIndexList(originTimestampColumn);
    this.timestampColumn = rowIndexList.stream().mapToLong(i -> originTimestampColumn[i]).toArray();

    final List<IMeasurementSchema> originMeasurementSchemaList = tablet.getSchemas();
    final String[] originMeasurementList = new String[originMeasurementSchemaList.size()];
    for (int i = 0; i < originMeasurementSchemaList.size(); i++) {
      originMeasurementList[i] = originMeasurementSchemaList.get(i).getMeasurementName();
    }

    generateColumnIndexMapper(
        originMeasurementList, originColumnIndex2FilteredColumnIndexMapperList);

    final int filteredColumnSize =
        Arrays.stream(originColumnIndex2FilteredColumnIndexMapperList)
            .filter(Objects::nonNull)
            .toArray()
            .length;

    this.measurementSchemaList = new MeasurementSchema[filteredColumnSize];
    this.columnNameStringList = new String[filteredColumnSize];
    this.valueColumnTypes = new ColumnCategory[filteredColumnSize];
    this.valueColumnDataTypes = new TSDataType[filteredColumnSize];
    this.valueColumns = new Object[filteredColumnSize];
    this.nullValueColumnBitmaps = new BitMap[filteredColumnSize];

    final String[] originColumnNameStringList = new String[originColumnSize];
    final ColumnCategory[] originColumnTypes = new ColumnCategory[originColumnSize];
    final TSDataType[] originValueColumnDataTypes = new TSDataType[originColumnSize];
    for (int i = 0; i < originColumnSize; i++) {
      originColumnNameStringList[i] = originMeasurementSchemaList.get(i).getMeasurementName();
      originColumnTypes[i] =
          tablet.getColumnTypes() != null && tablet.getColumnTypes().get(i) != null
              ? tablet.getColumnTypes().get(i)
              : ColumnCategory.FIELD;
      originValueColumnDataTypes[i] = originMeasurementSchemaList.get(i).getType();
    }
    final Object[] originValueColumns =
        tablet.getValues(); // we do not reduce value columns here by origin row size
    final BitMap[] originBitMapList =
        tablet.getBitMaps() == null
            ? IntStream.range(0, originColumnSize)
                .boxed()
                .map(o -> new BitMap(tablet.getMaxRowNumber()))
                .toArray(BitMap[]::new)
            : tablet.getBitMaps(); // We do not reduce bitmaps here by origin row size
    for (int i = 0; i < originBitMapList.length; i++) {
      if (originBitMapList[i] == null) {
        originBitMapList[i] = new BitMap(tablet.getMaxRowNumber());
      }
    }

    for (int i = 0; i < originColumnIndex2FilteredColumnIndexMapperList.length; i++) {
      if (originColumnIndex2FilteredColumnIndexMapperList[i] != null) {
        final int filteredColumnIndex = originColumnIndex2FilteredColumnIndexMapperList[i];
        this.measurementSchemaList[filteredColumnIndex] = originMeasurementSchemaList.get(i);
        this.columnNameStringList[filteredColumnIndex] = originColumnNameStringList[i];
        this.valueColumnTypes[filteredColumnIndex] =
            originColumnTypes[i] != null ? originColumnTypes[i] : ColumnCategory.FIELD;
        this.valueColumnDataTypes[filteredColumnIndex] = originValueColumnDataTypes[i];
        final BitMap bitMap = new BitMap(this.timestampColumn.length);
        if (Objects.isNull(originValueColumns[i])
            || Objects.isNull(originValueColumnDataTypes[i])) {
          fillNullValue(
              originValueColumnDataTypes[i],
              this.valueColumns,
              bitMap,
              filteredColumnIndex,
              rowIndexList.size());
        } else {
          this.valueColumns[filteredColumnIndex] =
              filterValueColumnsByRowIndexList(
                  originValueColumnDataTypes[i],
                  originValueColumns[i],
                  rowIndexList,
                  false,
                  originBitMapList[i],
                  bitMap);
        }
        this.nullValueColumnBitmaps[filteredColumnIndex] = bitMap;
      }
    }

    this.rowCount = this.timestampColumn.length;
    if (this.rowCount == 0 && LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Tablet({}) is parsed to zero rows according to the pattern({}) and time range [{}, {}], the corresponding source event({}) will be ignored.",
          tablet,
          getPattern(),
          this.sourceEvent.getStartTime(),
          this.sourceEvent.getEndTime(),
          this.sourceEvent);
    }
  }

  protected abstract void generateColumnIndexMapper(
      final String[] originMeasurementList,
      final Integer[] originColumnIndex2FilteredColumnIndexMapperList);

  private List<Integer> generateRowIndexList(final long[] originTimestampColumn) {
    final int rowCount = originTimestampColumn.length;
    if (Objects.isNull(sourceEvent) || !sourceEvent.shouldParseTime()) {
      return generateFullRowIndexList(rowCount);
    }

    final List<Integer> rowIndexList = new ArrayList<>();
    // We assume that `originTimestampColumn` is ordered.
    if (originTimestampColumn[originTimestampColumn.length - 1] < sourceEvent.getStartTime()
        || originTimestampColumn[0] > sourceEvent.getEndTime()) {
      return rowIndexList;
    }

    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
      if (sourceEvent.getStartTime() <= originTimestampColumn[rowIndex]
          && originTimestampColumn[rowIndex] <= sourceEvent.getEndTime()) {
        rowIndexList.add(rowIndex);
      }
    }

    return rowIndexList;
  }

  private static List<Integer> generateFullRowIndexList(final int rowCount) {
    if (rowCount <= CACHED_FULL_ROW_INDEX_LIST_ROW_COUNT_UPPER) {
      return CACHED_FULL_ROW_INDEX_LIST.get(rowCount);
    }
    return IntStream.range(0, rowCount).boxed().collect(Collectors.toList());
  }

  private static Object filterValueColumnsByRowIndexList(
      @NonNull final TSDataType type,
      @NonNull final Object originValueColumn,
      @NonNull final List<Integer> rowIndexList,
      final boolean isSingleOriginValueColumn,
      @NonNull final BitMap originNullValueColumnBitmap,
      @NonNull final BitMap nullValueColumnBitmap /* output parameters */) {
    switch (type) {
      case INT32:
        {
          final int[] intValueColumns =
              isSingleOriginValueColumn
                  ? new int[] {(int) originValueColumn}
                  : (int[]) originValueColumn;
          final int[] valueColumns = new int[rowIndexList.size()];
          for (int i = 0; i < rowIndexList.size(); ++i) {
            if (originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
              valueColumns[i] = 0;
              nullValueColumnBitmap.mark(i);
            } else {
              valueColumns[i] = intValueColumns[rowIndexList.get(i)];
            }
          }
          return valueColumns;
        }
      case DATE:
        {
          // Always store 'LocalDate[]' to help convert to tablet
          final LocalDate[] valueColumns = new LocalDate[rowIndexList.size()];
          if (isSingleOriginValueColumn && originValueColumn instanceof LocalDate
              || !isSingleOriginValueColumn && originValueColumn instanceof LocalDate[]) {
            // For tablet
            final LocalDate[] dateValueColumns =
                isSingleOriginValueColumn
                    ? new LocalDate[] {(LocalDate) originValueColumn}
                    : (LocalDate[]) originValueColumn;

            for (int i = 0; i < rowIndexList.size(); ++i) {
              if (originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
                valueColumns[i] = EMPTY_LOCALDATE;
                nullValueColumnBitmap.mark(i);
              } else {
                valueColumns[i] = dateValueColumns[rowIndexList.get(i)];
              }
            }
          } else {
            // For insertRowNode / insertTabletNode
            final int[] intValueColumns =
                isSingleOriginValueColumn
                    ? new int[] {(int) originValueColumn}
                    : (int[]) originValueColumn;
            for (int i = 0; i < rowIndexList.size(); ++i) {
              if (originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
                valueColumns[i] = EMPTY_LOCALDATE;
                nullValueColumnBitmap.mark(i);
              } else {
                valueColumns[i] =
                    DateUtils.parseIntToLocalDate(intValueColumns[rowIndexList.get(i)]);
              }
            }
          }
          return valueColumns;
        }
      case INT64:
      case TIMESTAMP:
        {
          final long[] longValueColumns =
              isSingleOriginValueColumn
                  ? new long[] {(long) originValueColumn}
                  : (long[]) originValueColumn;
          final long[] valueColumns = new long[rowIndexList.size()];
          for (int i = 0; i < rowIndexList.size(); ++i) {
            if (originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
              valueColumns[i] = 0L;
              nullValueColumnBitmap.mark(i);
            } else {
              valueColumns[i] = longValueColumns[rowIndexList.get(i)];
            }
          }
          return valueColumns;
        }
      case FLOAT:
        {
          final float[] floatValueColumns =
              isSingleOriginValueColumn
                  ? new float[] {(float) originValueColumn}
                  : (float[]) originValueColumn;
          final float[] valueColumns = new float[rowIndexList.size()];
          for (int i = 0; i < rowIndexList.size(); ++i) {
            if (originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
              valueColumns[i] = 0F;
              nullValueColumnBitmap.mark(i);
            } else {
              valueColumns[i] = floatValueColumns[rowIndexList.get(i)];
            }
          }
          return valueColumns;
        }
      case DOUBLE:
        {
          final double[] doubleValueColumns =
              isSingleOriginValueColumn
                  ? new double[] {(double) originValueColumn}
                  : (double[]) originValueColumn;
          final double[] valueColumns = new double[rowIndexList.size()];
          for (int i = 0; i < rowIndexList.size(); ++i) {
            if (originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
              valueColumns[i] = 0D;
              nullValueColumnBitmap.mark(i);
            } else {
              valueColumns[i] = doubleValueColumns[rowIndexList.get(i)];
            }
          }
          return valueColumns;
        }
      case BOOLEAN:
        {
          final boolean[] booleanValueColumns =
              isSingleOriginValueColumn
                  ? new boolean[] {(boolean) originValueColumn}
                  : (boolean[]) originValueColumn;
          final boolean[] valueColumns = new boolean[rowIndexList.size()];
          for (int i = 0; i < rowIndexList.size(); ++i) {
            if (originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
              valueColumns[i] = false;
              nullValueColumnBitmap.mark(i);
            } else {
              valueColumns[i] = booleanValueColumns[rowIndexList.get(i)];
            }
          }
          return valueColumns;
        }
      case TEXT:
      case BLOB:
      case STRING:
        {
          final Binary[] binaryValueColumns =
              isSingleOriginValueColumn
                  ? new Binary[] {(Binary) originValueColumn}
                  : (Binary[]) originValueColumn;
          final Binary[] valueColumns = new Binary[rowIndexList.size()];
          for (int i = 0; i < rowIndexList.size(); ++i) {
            if (Objects.isNull(binaryValueColumns[rowIndexList.get(i)])
                || Objects.isNull(binaryValueColumns[rowIndexList.get(i)].getValues())
                || originNullValueColumnBitmap.isMarked(rowIndexList.get(i))) {
              valueColumns[i] = Binary.EMPTY_VALUE;
              nullValueColumnBitmap.mark(i);
            } else {
              valueColumns[i] = new Binary(binaryValueColumns[rowIndexList.get(i)].getValues());
            }
          }
          return valueColumns;
        }
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", type));
    }
  }

  private void fillNullValue(
      final TSDataType type,
      final Object[] valueColumns,
      final BitMap nullValueColumnBitmap,
      final int columnIndex,
      final int rowSize) {
    nullValueColumnBitmap.markAll();
    if (Objects.isNull(type)) {
      return;
    }
    switch (type) {
      case TIMESTAMP:
      case INT64:
        valueColumns[columnIndex] = new long[rowSize];
        break;
      case INT32:
        valueColumns[columnIndex] = new int[rowSize];
        break;
      case DOUBLE:
        valueColumns[columnIndex] = new double[rowSize];
        break;
      case FLOAT:
        valueColumns[columnIndex] = new float[rowSize];
        break;
      case BOOLEAN:
        valueColumns[columnIndex] = new boolean[rowSize];
        break;
      case DATE:
        final LocalDate[] dates = new LocalDate[rowSize];
        Arrays.fill(dates, EMPTY_LOCALDATE);
        valueColumns[columnIndex] = dates;
        break;
      case TEXT:
      case BLOB:
      case STRING:
        final Binary[] columns = new Binary[rowSize];
        Arrays.fill(columns, Binary.EMPTY_VALUE);
        valueColumns[columnIndex] = columns;
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", type));
    }
  }

  ////////////////////////////  process  ////////////////////////////

  public abstract List<TabletInsertionEvent> processRowByRow(
      final BiConsumer<Row, RowCollector> consumer);

  public abstract List<TabletInsertionEvent> processTablet(
      final BiConsumer<Tablet, RowCollector> consumer);

  public abstract Tablet convertToTablet();
}
