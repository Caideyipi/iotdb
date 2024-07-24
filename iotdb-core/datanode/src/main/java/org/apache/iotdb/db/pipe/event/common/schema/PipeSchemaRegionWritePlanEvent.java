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

package org.apache.iotdb.db.pipe.event.common.schema;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeWritePlanEvent;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;

public class PipeSchemaRegionWritePlanEvent extends PipeWritePlanEvent {

  private PlanNode planNode;
  private DeletionResource deletionResource;

  public PipeSchemaRegionWritePlanEvent() {
    // Used for deserialization
    this(null, false);
  }

  public PipeSchemaRegionWritePlanEvent(final PlanNode planNode, final boolean isGeneratedByPipe) {
    this(planNode, null, 0, null, null, isGeneratedByPipe);
  }

  public PipeSchemaRegionWritePlanEvent(
      final PlanNode planNode,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipePattern pattern,
      final boolean isGeneratedByPipe) {
    super(pipeName, creationTime, pipeTaskMeta, pattern, isGeneratedByPipe);
    this.planNode = planNode;
  }

  public PlanNode getPlanNode() {
    return planNode;
  }

  public DeletionResource getDeletionResource() {
    return deletionResource;
  }

  public void setDeletionResource(DeletionResource deletionResource) {
    this.deletionResource = deletionResource;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    if (deletionResource != null) {
      deletionResource.persistSelf();
    }
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    if (deletionResource != null) {
      deletionResource.removeSelf();
      // Resolve circular reference to let GC reclaim them all.
      deletionResource.releaseSelf();
      deletionResource = null;
    }
    return true;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipePattern pattern,
      final long startTime,
      final long endTime) {
    return new PipeSchemaRegionWritePlanEvent(
        planNode, pipeName, creationTime, pipeTaskMeta, pattern, isGeneratedByPipe);
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    final ByteBuffer planBuffer = planNode.serializeToByteBuffer();
    final ByteBuffer result = ByteBuffer.allocate(Byte.BYTES * 2 + planBuffer.limit());
    ReadWriteIOUtils.write(PipeSchemaSerializableEventType.SCHEMA_WRITE_PLAN.getType(), result);
    ReadWriteIOUtils.write(isGeneratedByPipe, result);
    result.put(planBuffer);
    return result;
  }

  @Override
  public void deserializeFromByteBuffer(final ByteBuffer buffer) {
    isGeneratedByPipe = ReadWriteIOUtils.readBool(buffer);
    planNode = PlanNodeType.deserialize(buffer);
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format("PipeSchemaRegionWritePlanEvent{planNode=%s}", planNode)
        + " - "
        + super.toString();
  }

  @Override
  public String coreReportMessage() {
    return String.format("PipeSchemaRegionWritePlanEvent{planNode=%s}", planNode)
        + " - "
        + super.coreReportMessage();
  }
}
