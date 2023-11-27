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

package org.apache.iotdb.commons.pipe.task.meta;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SchemaProgressIndex;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeExceptionType;
import org.apache.iotdb.commons.pipe.task.meta.compatibility.PipeRuntimeMetaVersion;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PipeRuntimeMeta {

  /////////////////////////////// Fields ///////////////////////////////

  private final AtomicReference<PipeStatus> status = new AtomicReference<>(PipeStatus.STOPPED);

  // All the progressIndexes of the pipe.
  /**
   * {@link SchemaProgressIndex} for schema transmission on ConfigNode, always {@link
   * MinimumProgressIndex} if the pipe has no relation to schema synchronization.
   */
  private final AtomicReference<ProgressIndex> configProgressIndex =
      new AtomicReference<>(MinimumProgressIndex.INSTANCE);

  /**
   * Used to store progress for schema transmission on DataRegion, usually updated since the data
   * synchronization is basic function of pipe.
   */
  private Map<TConsensusGroupId, PipeTaskMeta> dataRegionId2TaskMetaMap;

  /**
   * Used to store {@link SchemaProgressIndex} for schema transmission on SchemaRegion, always
   * {@link MinimumProgressIndex} if the pipe has no relation to schema synchronization.
   */
  private final Map<TConsensusGroupId, PipeTaskMeta> schemaRegionId2TaskMetaMap;

  /**
   * Stores the newest exceptions encountered group by dataNodes.
   *
   * <p>The exceptions are all instances of:
   *
   * <p>1. {@link PipeRuntimeCriticalException}, to record the failure of pushing pipeMeta, and will
   * result in the halt of pipe execution.
   *
   * <p>2. {@link PipeRuntimeConnectorCriticalException}, to record the exception reported by other
   * pipes sharing the same connector, and will stop the pipe likewise.
   */
  private Map<Integer, PipeRuntimeException> dataNodeId2PipeRuntimeExceptionMap =
      new ConcurrentHashMap<>();

  private final AtomicLong exceptionsClearTime = new AtomicLong(Long.MIN_VALUE);

  private final AtomicBoolean isStoppedByRuntimeException = new AtomicBoolean(false);

  /////////////////////////////// Initializer ///////////////////////////////

  public PipeRuntimeMeta() {
    dataRegionId2TaskMetaMap = new ConcurrentHashMap<>();
    schemaRegionId2TaskMetaMap = new ConcurrentHashMap<>();
  }

  public PipeRuntimeMeta(
      Map<TConsensusGroupId, PipeTaskMeta> dataRegionId2TaskMetaMap,
      Map<TConsensusGroupId, PipeTaskMeta> schemaRegionId2TaskMetaMap) {
    this.dataRegionId2TaskMetaMap = new ConcurrentHashMap<>(dataRegionId2TaskMetaMap);
    this.schemaRegionId2TaskMetaMap = new ConcurrentHashMap<>(schemaRegionId2TaskMetaMap);
  }

  /////////////////////////////// Normal getter & setter ///////////////////////////////

  public AtomicReference<PipeStatus> getStatus() {
    return status;
  }

  public ProgressIndex getConfigProgressIndex() {
    return configProgressIndex.get();
  }

  public ProgressIndex updateConfigProgressIndex(ProgressIndex updateIndex) {
    return configProgressIndex.updateAndGet(
        index -> index.updateToMinimumIsAfterProgressIndex(updateIndex));
  }

  public Map<TConsensusGroupId, PipeTaskMeta> getDataRegionId2TaskMetaMap() {
    return dataRegionId2TaskMetaMap;
  }

  public Map<TConsensusGroupId, PipeTaskMeta> getSchemaRegionId2TaskMetaMap() {
    return schemaRegionId2TaskMetaMap;
  }

  public Map<Integer, PipeRuntimeException> getDataNodeId2PipeRuntimeExceptionMap() {
    return dataNodeId2PipeRuntimeExceptionMap;
  }

  public long getExceptionsClearTime() {
    return exceptionsClearTime.get();
  }

  public void setExceptionsClearTime(long exceptionsClearTime) {
    if (exceptionsClearTime > this.getExceptionsClearTime()) {
      this.exceptionsClearTime.set(exceptionsClearTime);
    }
  }

  public boolean getIsStoppedByRuntimeException() {
    return isStoppedByRuntimeException.get();
  }

  public void setIsStoppedByRuntimeException(boolean isStoppedByRuntimeException) {
    this.isStoppedByRuntimeException.set(isStoppedByRuntimeException);
  }

  /////////////////////////////// Serialization & deserialization ///////////////////////////////

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(OutputStream outputStream) throws IOException {
    PipeRuntimeMetaVersion.VERSION_3.serialize(outputStream);

    ReadWriteIOUtils.write(status.get().getType(), outputStream);

    configProgressIndex.get().serialize(outputStream);

    // Avoid concurrent modification
    final Map<TConsensusGroupId, PipeTaskMeta> dataRegionId2TaskMetaMapView =
        new HashMap<>(dataRegionId2TaskMetaMap);
    ReadWriteIOUtils.write(dataRegionId2TaskMetaMapView.size(), outputStream);
    for (Map.Entry<TConsensusGroupId, PipeTaskMeta> entry :
        dataRegionId2TaskMetaMapView.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getId(), outputStream);
      entry.getValue().serialize(outputStream);
    }

    final Map<TConsensusGroupId, PipeTaskMeta> schemaRegionId2TaskMetaMapView =
        new HashMap<>(schemaRegionId2TaskMetaMap);
    ReadWriteIOUtils.write(schemaRegionId2TaskMetaMapView.size(), outputStream);
    for (Map.Entry<TConsensusGroupId, PipeTaskMeta> entry :
        schemaRegionId2TaskMetaMapView.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getId(), outputStream);
      entry.getValue().serialize(outputStream);
    }

    // Avoid concurrent modification
    final Map<Integer, PipeRuntimeException> dataNodeId2PipeRuntimeExceptionMapView =
        new HashMap<>(dataNodeId2PipeRuntimeExceptionMap);
    ReadWriteIOUtils.write(dataNodeId2PipeRuntimeExceptionMapView.size(), outputStream);
    for (Map.Entry<Integer, PipeRuntimeException> entry :
        dataNodeId2PipeRuntimeExceptionMapView.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().serialize(outputStream);
    }

    ReadWriteIOUtils.write(exceptionsClearTime.get(), outputStream);
    ReadWriteIOUtils.write(isStoppedByRuntimeException.get(), outputStream);
  }

  @Nonnull
  public static PipeRuntimeMeta deserialize(InputStream inputStream) throws IOException {
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta();

    pipeRuntimeMeta.status.set(PipeStatus.getPipeStatus(ReadWriteIOUtils.readByte(inputStream)));

    pipeRuntimeMeta.configProgressIndex.set(ProgressIndexType.deserializeFrom(inputStream));

    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.dataRegionId2TaskMetaMap.put(
          new TConsensusGroupId(
              TConsensusGroupType.DataRegion, ReadWriteIOUtils.readInt(inputStream)),
          PipeTaskMeta.deserialize(PipeRuntimeMetaVersion.VERSION_2, inputStream));
    }

    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.schemaRegionId2TaskMetaMap.put(
          new TConsensusGroupId(
              TConsensusGroupType.SchemaRegion, ReadWriteIOUtils.readInt(inputStream)),
          PipeTaskMeta.deserialize(PipeRuntimeMetaVersion.VERSION_2, inputStream));
    }

    size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.dataNodeId2PipeRuntimeExceptionMap.put(
          ReadWriteIOUtils.readInt(inputStream),
          PipeRuntimeExceptionType.deserializeFrom(PipeRuntimeMetaVersion.VERSION_2, inputStream));
    }

    pipeRuntimeMeta.exceptionsClearTime.set(ReadWriteIOUtils.readLong(inputStream));
    pipeRuntimeMeta.isStoppedByRuntimeException.set(ReadWriteIOUtils.readBool(inputStream));

    return pipeRuntimeMeta;
  }

  @Nonnull
  public static PipeRuntimeMeta deserialize(ByteBuffer byteBuffer) {
    final PipeRuntimeMeta pipeRuntimeMeta = new PipeRuntimeMeta();

    pipeRuntimeMeta.status.set(PipeStatus.getPipeStatus(ReadWriteIOUtils.readByte(byteBuffer)));

    pipeRuntimeMeta.configProgressIndex.set(ProgressIndexType.deserializeFrom(byteBuffer));

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.dataRegionId2TaskMetaMap.put(
          new TConsensusGroupId(
              TConsensusGroupType.DataRegion, ReadWriteIOUtils.readInt(byteBuffer)),
          PipeTaskMeta.deserialize(PipeRuntimeMetaVersion.VERSION_2, byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.schemaRegionId2TaskMetaMap.put(
          new TConsensusGroupId(
              TConsensusGroupType.SchemaRegion, ReadWriteIOUtils.readInt(byteBuffer)),
          PipeTaskMeta.deserialize(PipeRuntimeMetaVersion.VERSION_2, byteBuffer));
    }

    size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      pipeRuntimeMeta.dataNodeId2PipeRuntimeExceptionMap.put(
          ReadWriteIOUtils.readInt(byteBuffer),
          PipeRuntimeExceptionType.deserializeFrom(PipeRuntimeMetaVersion.VERSION_2, byteBuffer));
    }

    pipeRuntimeMeta.exceptionsClearTime.set(ReadWriteIOUtils.readLong(byteBuffer));
    pipeRuntimeMeta.isStoppedByRuntimeException.set(ReadWriteIOUtils.readBool(byteBuffer));

    return pipeRuntimeMeta;
  }

  /////////////////////////////// Compatibility ///////////////////////////////

  // DO NOT CALL IT, unless from the former versions
  public void setDataNodeId2PipeRuntimeExceptionMap(
      Map<Integer, PipeRuntimeException> dataNodeId2PipeRuntimeExceptionMap) {
    this.dataNodeId2PipeRuntimeExceptionMap = dataNodeId2PipeRuntimeExceptionMap;
  }

  public void setDataRegionId2TaskMetaMap(
      Map<TConsensusGroupId, PipeTaskMeta> dataRegionId2TaskMetaMap) {
    this.dataRegionId2TaskMetaMap = dataRegionId2TaskMetaMap;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipeRuntimeMeta that = (PipeRuntimeMeta) o;
    return Objects.equals(status.get().getType(), that.status.get().getType())
        && dataRegionId2TaskMetaMap.equals(that.dataRegionId2TaskMetaMap)
        && schemaRegionId2TaskMetaMap.equals(that.schemaRegionId2TaskMetaMap)
        && dataNodeId2PipeRuntimeExceptionMap.equals(that.dataNodeId2PipeRuntimeExceptionMap)
        && exceptionsClearTime.get() == that.exceptionsClearTime.get()
        && isStoppedByRuntimeException.get() == that.isStoppedByRuntimeException.get();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        status,
        dataRegionId2TaskMetaMap,
        schemaRegionId2TaskMetaMap,
        dataNodeId2PipeRuntimeExceptionMap,
        exceptionsClearTime.get(),
        isStoppedByRuntimeException.get());
  }

  @Override
  public String toString() {
    return "PipeRuntimeMeta{"
        + "status="
        + status
        + ", dataRegionId2TaskMetaMap="
        + dataRegionId2TaskMetaMap
        + ", schemaRegionId2TaskMetaMap="
        + schemaRegionId2TaskMetaMap
        + dataNodeId2PipeRuntimeExceptionMap
        + ", exceptionsClearTime="
        + exceptionsClearTime.get()
        + ", isStoppedByRuntimeException="
        + isStoppedByRuntimeException.get()
        + "}";
  }
}
