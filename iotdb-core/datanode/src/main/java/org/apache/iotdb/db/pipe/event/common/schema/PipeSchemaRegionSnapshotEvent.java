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

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.resource.snapshot.PipeDataNodeSnapshotResourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PipeSchemaRegionSnapshotEvent extends EnrichedEvent {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSchemaRegionSnapshotEvent.class);

  private String snapshotPath;

  public PipeSchemaRegionSnapshotEvent(
      String snapshotPath, String pipeName, PipeTaskMeta pipeTaskMeta, String pattern) {
    super(pipeName, pipeTaskMeta, pattern, Long.MIN_VALUE, Long.MAX_VALUE);
    this.snapshotPath = snapshotPath;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    try {
      snapshotPath =
          PipeDataNodeSnapshotResourceManager.getInstance().increaseSnapshotReference(snapshotPath);
      return true;
    } catch (IOException e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for snapshot %s error. Holder Message: %s",
              snapshotPath, holderMessage),
          e);
      return false;
    }
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    try {
      PipeDataNodeSnapshotResourceManager.getInstance().decreaseSnapshotReference(snapshotPath);
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for snapshot %s error. Holder Message: %s",
              snapshotPath, holderMessage),
          e);
      return false;
    }
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return null;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName, PipeTaskMeta pipeTaskMeta, String pattern, long startTime, long endTime) {
    return new PipeSchemaRegionSnapshotEvent(snapshotPath, pipeName, pipeTaskMeta, pattern);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return false;
  }

  @Override
  public boolean isEventTimeOverlappedWithTimeRange() {
    return true;
  }
}
