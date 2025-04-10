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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class InsertNode extends SearchNode {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /**
   * if use id table, this filed is id form of device path <br>
   * if not, this filed is device path<br>
   * or table name for table-model insertions.
   */
  protected PartialPath targetPath;

  protected boolean isAligned;
  protected MeasurementSchema[] measurementSchemas;
  protected String[] measurements;
  protected TSDataType[] dataTypes;

  protected TsTableColumnCategory[] columnCategories;
  protected List<Integer> idColumnIndices;
  protected int measurementColumnCnt = -1;

  protected int failedMeasurementNumber = 0;

  /**
   * device id reference, for reuse device id in both id table and memtable <br>
   * used in memtable
   */
  protected IDeviceID deviceID;

  protected boolean isGeneratedByRemoteConsensusLeader = false;

  /** Physical address of data region after splitting */
  protected TRegionReplicaSet dataRegionReplicaSet;

  protected ProgressIndex progressIndex;

  protected long memorySize;

  private static final DeviceIDFactory deviceIDFactory = DeviceIDFactory.getInstance();

  protected InsertNode(PlanNodeId id) {
    super(id);
  }

  @Override
  public final SearchNode merge(List<SearchNode> searchNodes) {
    if (searchNodes.isEmpty()) {
      throw new IllegalArgumentException("insertNodes should never be empty");
    }
    if (searchNodes.size() == 1) {
      return searchNodes.get(0);
    }
    List<InsertNode> insertNodes =
        searchNodes.stream()
            .map(searchNode -> (InsertNode) searchNode)
            .collect(Collectors.toList());
    InsertNode result = mergeInsertNode(insertNodes);
    result.setSearchIndex(insertNodes.get(0).getSearchIndex());
    result.setTargetPath(insertNodes.get(0).getTargetPath());
    return result;
  }

  public abstract InsertNode mergeInsertNode(List<InsertNode> insertNodes);

  protected InsertNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      String[] measurements,
      TSDataType[] dataTypes) {
    this(id, devicePath, isAligned, measurements, dataTypes, null);
  }

  protected InsertNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      String[] measurements,
      TSDataType[] dataTypes,
      TsTableColumnCategory[] columnCategories) {
    super(id);
    this.targetPath = devicePath;
    this.isAligned = isAligned;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    setColumnCategories(columnCategories);
  }

  public TRegionReplicaSet getDataRegionReplicaSet() {
    return dataRegionReplicaSet;
  }

  public void setDataRegionReplicaSet(TRegionReplicaSet dataRegionReplicaSet) {
    this.dataRegionReplicaSet = dataRegionReplicaSet;
  }

  public PartialPath getTargetPath() {
    return targetPath;
  }

  public void setTargetPath(PartialPath targetPath) {
    this.targetPath = targetPath;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  public MeasurementSchema[] getMeasurementSchemas() {
    return measurementSchemas;
  }

  public void setMeasurementSchemas(MeasurementSchema[] measurementSchemas) {
    this.measurementSchemas = measurementSchemas;
  }

  public String[] getMeasurements() {
    return measurements;
  }

  public int measureColumnCnt() {
    if (columnCategories == null) {
      return measurements.length;
    }
    return (int)
        Arrays.stream(columnCategories).filter(col -> col == TsTableColumnCategory.FIELD).count();
  }

  public boolean isValidMeasurement(int i) {
    return measurementSchemas != null
        && measurementSchemas[i] != null
        && (columnCategories == null || columnCategories[i] == TsTableColumnCategory.FIELD);
  }

  public void setMeasurements(String[] measurements) {
    this.measurements = measurements;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public int getMeasurementColumnCnt() {
    if (measurementColumnCnt == -1) {
      measurementColumnCnt = 0;
      if (measurementSchemas != null) {
        for (int i = 0; i < measurementSchemas.length; i++) {
          if (isValidMeasurement(i)) {
            measurementColumnCnt++;
          }
        }
      }
    }
    return measurementColumnCnt;
  }

  public TSDataType getDataType(int index) {
    return dataTypes[index];
  }

  public void setDataTypes(TSDataType[] dataTypes) {
    this.dataTypes = dataTypes;
  }

  public IDeviceID getDeviceID() {
    if (deviceID == null) {
      deviceID = deviceIDFactory.getDeviceID(targetPath);
    }
    return deviceID;
  }

  public void setDeviceID(IDeviceID deviceID) {
    this.deviceID = deviceID;
  }

  public boolean isDeviceIDExists() {
    return deviceID != null;
  }

  public boolean isGeneratedByRemoteConsensusLeader() {
    switch (config.getDataRegionConsensusProtocolClass()) {
      case ConsensusFactory.IOT_CONSENSUS:
      case ConsensusFactory.IOT_CONSENSUS_V2:
      case ConsensusFactory.RATIS_CONSENSUS:
        return isGeneratedByRemoteConsensusLeader;
      case ConsensusFactory.SIMPLE_CONSENSUS:
        return false;
    }
    return false;
  }

  @Override
  public void markAsGeneratedByRemoteConsensusLeader() {
    isGeneratedByRemoteConsensusLeader = true;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    throw new NotImplementedException("serializeAttributes of InsertNode is not implemented");
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    throw new NotImplementedException("serializeAttributes of InsertNode is not implemented");
  }

  // region Serialization methods for WAL

  /** Serialized size of measurement schemas, ignoring failed time series */
  protected int serializeMeasurementSchemasSize() {
    int byteLen = 0;
    for (int i = 0; i < measurements.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      byteLen += WALWriteUtils.sizeToWrite(measurementSchemas[i]);
    }
    return byteLen;
  }

  /** Serialize measurement schemas, ignoring failed time series */
  protected void serializeMeasurementSchemasToWAL(IWALByteBufferView buffer) {
    for (int i = 0; i < measurements.length; i++) {
      // ignore failed partial insert
      if (measurements[i] == null) {
        continue;
      }
      WALWriteUtils.write(measurementSchemas[i], buffer);
    }
  }

  /**
   * Deserialize measurement schemas. Make sure the measurement schemas and measurements have been
   * created before calling this
   */
  protected void deserializeMeasurementSchemas(DataInputStream stream) throws IOException {
    for (int i = 0; i < measurements.length; i++) {
      measurementSchemas[i] = MeasurementSchema.deserializeFrom(stream);
      measurements[i] = measurementSchemas[i].getMeasurementName();
      dataTypes[i] = measurementSchemas[i].getType();
    }
  }

  protected void deserializeMeasurementSchemas(ByteBuffer buffer) {
    for (int i = 0; i < measurements.length; i++) {
      measurementSchemas[i] = MeasurementSchema.deserializeFrom(buffer);
      measurements[i] = measurementSchemas[i].getMeasurementName();
    }
  }

  // endregion

  public TRegionReplicaSet getRegionReplicaSet() {
    return dataRegionReplicaSet;
  }

  public abstract long getMinTime();

  // region partial insert
  public void markFailedMeasurement(int index) {
    throw new UnsupportedOperationException();
  }

  public boolean hasValidMeasurements() {
    for (Object o : measurements) {
      if (o != null) {
        return true;
      }
    }
    return false;
  }

  public void setFailedMeasurementNumber(int failedMeasurementNumber) {
    this.failedMeasurementNumber = failedMeasurementNumber;
  }

  public int getFailedMeasurementNumber() {
    return failedMeasurementNumber;
  }

  public boolean allMeasurementFailed() {
    if (measurements != null) {
      return failedMeasurementNumber
          >= measurements.length - (idColumnIndices == null ? 0 : idColumnIndices.size());
    }
    return true;
  }

  // endregion

  // region progress index

  @Override
  public ProgressIndex getProgressIndex() {
    return progressIndex;
  }

  @Override
  public void setProgressIndex(ProgressIndex progressIndex) {
    this.progressIndex = progressIndex;
  }

  // endregion

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    InsertNode that = (InsertNode) o;
    return isAligned == that.isAligned
        && Objects.equals(targetPath, that.targetPath)
        && Arrays.equals(measurementSchemas, that.measurementSchemas)
        && Arrays.equals(measurements, that.measurements)
        && Arrays.equals(dataTypes, that.dataTypes)
        && Objects.equals(deviceID, that.deviceID)
        && Objects.equals(dataRegionReplicaSet, that.dataRegionReplicaSet);
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(super.hashCode(), targetPath, isAligned, deviceID, dataRegionReplicaSet);
    result = 31 * result + Arrays.hashCode(measurementSchemas);
    result = 31 * result + Arrays.hashCode(measurements);
    result = 31 * result + Arrays.hashCode(dataTypes);
    return result;
  }

  public TsTableColumnCategory[] getColumnCategories() {
    return columnCategories;
  }

  public void setColumnCategories(TsTableColumnCategory[] columnCategories) {
    this.columnCategories = columnCategories;
    if (columnCategories != null) {
      idColumnIndices = new ArrayList<>();
      for (int i = 0; i < columnCategories.length; i++) {
        if (columnCategories[i].equals(TsTableColumnCategory.TAG)) {
          idColumnIndices.add(i);
        }
      }
    }
  }

  public String getTableName() {
    return null;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  public String[] getRawMeasurements() {
    String[] measurements = getMeasurements();
    MeasurementSchema[] measurementSchemas = getMeasurementSchemas();
    String[] rawMeasurements = new String[measurements.length];
    for (int i = 0; i < measurements.length; i++) {
      if (measurementSchemas[i] != null) {
        // get raw measurement rather than alias
        rawMeasurements[i] = measurementSchemas[i].getMeasurementName();
      } else {
        rawMeasurements[i] = measurements[i];
      }
    }
    return rawMeasurements;
  }

  protected PartialPath readTargetPath(ByteBuffer buffer) throws IllegalPathException {
    return DataNodeDevicePathCache.getInstance()
        .getPartialPath(ReadWriteIOUtils.readString(buffer));
  }

  protected PartialPath readTargetPath(DataInputStream stream)
      throws IllegalPathException, IOException {
    return DataNodeDevicePathCache.getInstance()
        .getPartialPath(ReadWriteIOUtils.readString(stream));
  }

  @Override
  public long getMemorySize() {
    if (memorySize == 0) {
      memorySize = InsertNodeMemoryEstimator.sizeOf(this);
    }
    return memorySize;
  }
}
