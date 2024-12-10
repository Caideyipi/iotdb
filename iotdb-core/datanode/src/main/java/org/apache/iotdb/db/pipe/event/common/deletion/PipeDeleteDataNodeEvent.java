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

package org.apache.iotdb.db.pipe.event.common.deletion;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.SerializableEvent;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.Optional;

public class PipeDeleteDataNodeEvent extends EnrichedEvent implements SerializableEvent {
  private AbstractDeleteDataNode deleteDataNode;
  private DeletionResource deletionResource;
  private boolean isGeneratedByPipe;
  private ProgressIndex progressIndex;
  private transient String database;

  public PipeDeleteDataNodeEvent() {
    // Used for deserialization
    this(null, false, null);
  }

  public PipeDeleteDataNodeEvent(
      final AbstractDeleteDataNode deleteDataNode,
      final boolean isGeneratedByPipe,
      final String database) {
    this(deleteDataNode, null, 0, null, null, null, isGeneratedByPipe, database);
  }

  public PipeDeleteDataNodeEvent(
      final AbstractDeleteDataNode deleteDataNode,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final boolean isGeneratedByPipe,
      final String database) {
    super(
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.deleteDataNode = deleteDataNode;
    this.database = database;
    Optional.ofNullable(deleteDataNode)
        .ifPresent(node -> this.progressIndex = deleteDataNode.getProgressIndex());
  }

  public AbstractDeleteDataNode getDeleteDataNode() {
    return deleteDataNode;
  }

  public DeletionResource getDeletionResource() {
    return deletionResource;
  }

  public void setDeletionResource(final DeletionResource deletionResource) {
    this.deletionResource = deletionResource;
  }

  public String getDatabase() {
    return database;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    return true;
  }

  @Override
  public void onCommitted() {
    super.onCommitted();
    if (deletionResource != null) {
      deletionResource.decreaseReference();
    }
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return progressIndex == null ? MinimumProgressIndex.INSTANCE : progressIndex;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime) {
    return new PipeDeleteDataNodeEvent(
        deleteDataNode,
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        isGeneratedByPipe,
        database);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    return true;
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    return true;
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    final ByteBuffer planBuffer = deleteDataNode.serializeToByteBuffer();
    final ByteBuffer result = ByteBuffer.allocate(Byte.BYTES + planBuffer.limit());
    ReadWriteIOUtils.write(isGeneratedByPipe, result);
    result.put(planBuffer);
    return result;
  }

  @Override
  public void deserializeFromByteBuffer(final ByteBuffer buffer) {
    isGeneratedByPipe = ReadWriteIOUtils.readBool(buffer);
    deleteDataNode = (DeleteDataNode) PlanNodeType.deserialize(buffer);
    progressIndex = deleteDataNode.getProgressIndex();
  }

  public static PipeDeleteDataNodeEvent deserialize(final ByteBuffer buffer) {
    final PipeDeleteDataNodeEvent event = new PipeDeleteDataNodeEvent();
    event.deserializeFromByteBuffer(buffer);
    return event;
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipDeleteDataNodeEvent{progressIndex=%s, isGeneratedByPipe=%s}",
            progressIndex, isGeneratedByPipe)
        + " - "
        + super.toString();
  }

  @Override
  public String coreReportMessage() {
    return String.format(
            "PipeDeleteDataNodeEvent{progressIndex=%s, isGeneratedByPipe=%s}",
            progressIndex, isGeneratedByPipe)
        + " - "
        + super.coreReportMessage();
  }
}
