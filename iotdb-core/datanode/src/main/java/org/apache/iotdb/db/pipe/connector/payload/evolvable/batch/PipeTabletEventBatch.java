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

package org.apache.iotdb.db.pipe.connector.payload.evolvable.batch;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.exception.write.WriteProcessException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class PipeTabletEventBatch implements AutoCloseable {

  private final List<EnrichedEvent> events = new ArrayList<>();

  private final int maxDelayInMs;
  private long firstEventProcessingTime = Long.MIN_VALUE;

  protected long totalBufferSize = 0;

  protected volatile boolean isClosed = false;

  protected PipeTabletEventBatch(final int maxDelayInMs) {
    this.maxDelayInMs = maxDelayInMs;
  }

  /**
   * Try offer {@link Event} into batch if the given {@link Event} is not duplicated.
   *
   * @param event the given {@link Event}
   * @return {@code true} if the batch can be transferred
   */
  synchronized boolean onEvent(final TabletInsertionEvent event)
      throws WALPipeException, IOException, WriteProcessException {
    if (isClosed || !(event instanceof EnrichedEvent)) {
      return false;
    }

    // The deduplication logic here is to avoid the accumulation of
    // the same event in a batch when retrying.
    if (events.isEmpty() || !Objects.equals(events.get(events.size() - 1), event)) {
      // We increase the reference count for this event to determine if the event may be released.
      if (((EnrichedEvent) event)
          .increaseReferenceCount(PipeTransferBatchReqBuilder.class.getName())) {
        events.add((EnrichedEvent) event);

        constructBatch(event);

        if (firstEventProcessingTime == Long.MIN_VALUE) {
          firstEventProcessingTime = System.currentTimeMillis();
        }
      } else {
        ((EnrichedEvent) event)
            .decreaseReferenceCount(PipeTransferBatchReqBuilder.class.getName(), false);
      }
    }

    return totalBufferSize >= getMaxBatchSizeInBytes()
        || System.currentTimeMillis() - firstEventProcessingTime >= maxDelayInMs;
  }

  protected abstract void constructBatch(final TabletInsertionEvent event)
      throws WALPipeException, IOException, WriteProcessException;

  protected abstract long getMaxBatchSizeInBytes();

  public synchronized void onSuccess() {
    events.clear();

    totalBufferSize = 0;

    firstEventProcessingTime = Long.MIN_VALUE;
  }

  @Override
  public synchronized void close() {
    isClosed = true;

    clearEventsReferenceCount(PipeTransferBatchReqBuilder.class.getName());
    events.clear();
  }

  public void decreaseEventsReferenceCount(final String holderMessage, final boolean shouldReport) {
    events.forEach(event -> event.decreaseReferenceCount(holderMessage, shouldReport));
  }

  private void clearEventsReferenceCount(final String holderMessage) {
    events.forEach(event -> event.clearReferenceCount(holderMessage));
  }

  public List<EnrichedEvent> deepCopyEvents() {
    return new ArrayList<>(events);
  }

  boolean isEmpty() {
    return events.isEmpty();
  }
}
