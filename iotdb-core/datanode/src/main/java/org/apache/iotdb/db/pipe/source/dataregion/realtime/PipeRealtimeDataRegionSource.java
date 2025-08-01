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

package org.apache.iotdb.db.pipe.source.dataregion.realtime;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.db.pipe.source.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeTsFileEpochProgressIndexKeeper;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.listener.PipeTimePartitionListener;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.apache.tsfile.utils.Pair;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_LOOSE_RANGE_ALL_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_LOOSE_RANGE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_LOOSE_RANGE_PATH_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_LOOSE_RANGE_TIME_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_MODS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_REALTIME_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.source.IoTDBSource.getSkipIfNoPrivileges;

public abstract class PipeRealtimeDataRegionSource implements PipeExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeRealtimeDataRegionSource.class);

  protected String pipeName;
  protected long creationTime;
  protected String dataRegionId;
  protected PipeTaskMeta pipeTaskMeta;

  protected boolean shouldExtractInsertion;
  protected boolean shouldExtractDeletion;

  protected TreePattern treePattern;
  protected TablePattern tablePattern;
  private boolean isDbNameCoveredByPattern = false;

  protected long realtimeDataExtractionStartTime = Long.MIN_VALUE; // Event time
  protected long realtimeDataExtractionEndTime = Long.MAX_VALUE; // Event time

  private boolean disableCheckingDataRegionTimePartitionCovering = false;
  private long startTimePartitionIdLowerBound; // calculated by realtimeDataExtractionStartTime
  private long endTimePartitionIdUpperBound; // calculated by realtimeDataExtractionEndTime

  // This variable is used to record the upper and lower bounds that the time partition ID
  // corresponding to this data region has ever reached. It may be updated by
  // PipeTimePartitionListener.
  private final AtomicReference<Pair<Long, Long>> dataRegionTimePartitionIdBound =
      new AtomicReference<>();

  protected boolean isForwardingPipeRequests;

  private boolean shouldTransferModFile; // Whether to transfer mods

  private boolean sloppyTimeRange; // true to disable time range filter after extraction
  private boolean sloppyPattern; // true to disable pattern filter after extraction

  // This queue is used to store pending events extracted by the method extract(). The method
  // supply() will poll events from this queue and send them to the next pipe plugin.
  protected final UnboundedBlockingPendingQueue<Event> pendingQueue =
      new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());

  protected final AtomicBoolean isClosed = new AtomicBoolean(false);

  protected String pipeID;
  private String taskID;
  protected String userName;
  protected boolean skipIfNoPrivileges = true;

  protected PipeRealtimeDataRegionSource() {
    // Do nothing
  }

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();

    try {
      realtimeDataExtractionStartTime =
          parameters.hasAnyAttributes(SOURCE_START_TIME_KEY, EXTRACTOR_START_TIME_KEY)
              ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                  parameters.getStringByKeys(SOURCE_START_TIME_KEY, EXTRACTOR_START_TIME_KEY))
              : Long.MIN_VALUE;
      realtimeDataExtractionEndTime =
          parameters.hasAnyAttributes(SOURCE_END_TIME_KEY, EXTRACTOR_END_TIME_KEY)
              ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                  parameters.getStringByKeys(SOURCE_END_TIME_KEY, EXTRACTOR_END_TIME_KEY))
              : Long.MAX_VALUE;
      if (realtimeDataExtractionStartTime > realtimeDataExtractionEndTime) {
        throw new PipeParameterNotValidException(
            String.format(
                "%s (%s) [%s] should be less than or equal to %s (%s) [%s].",
                SOURCE_START_TIME_KEY,
                EXTRACTOR_START_TIME_KEY,
                realtimeDataExtractionStartTime,
                SOURCE_END_TIME_KEY,
                EXTRACTOR_END_TIME_KEY,
                realtimeDataExtractionEndTime));
      }
    } catch (final PipeParameterNotValidException e) {
      throw e;
    } catch (final Exception e) {
      // compatible with the current validation framework
      throw new PipeParameterNotValidException(e.getMessage());
    }

    final String extractorRealtimeLooseRangeValue =
        parameters
            .getStringOrDefault(
                Arrays.asList(EXTRACTOR_REALTIME_LOOSE_RANGE_KEY, SOURCE_REALTIME_LOOSE_RANGE_KEY),
                EXTRACTOR_REALTIME_LOOSE_RANGE_DEFAULT_VALUE)
            .trim();
    if (EXTRACTOR_REALTIME_LOOSE_RANGE_ALL_VALUE.equalsIgnoreCase(
        extractorRealtimeLooseRangeValue)) {
      sloppyTimeRange = true;
      sloppyPattern = true;
    } else {
      final Set<String> sloppyOptionSet =
          Arrays.stream(extractorRealtimeLooseRangeValue.split(","))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .map(String::toLowerCase)
              .collect(Collectors.toSet());
      sloppyTimeRange = sloppyOptionSet.remove(EXTRACTOR_REALTIME_LOOSE_RANGE_TIME_VALUE);
      sloppyPattern = sloppyOptionSet.remove(EXTRACTOR_REALTIME_LOOSE_RANGE_PATH_VALUE);
      if (!sloppyOptionSet.isEmpty()) {
        throw new PipeParameterNotValidException(
            String.format(
                "Parameters in set %s are not allowed in 'realtime.loose-range'", sloppyOptionSet));
      }
    }
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();

    final Pair<Boolean, Boolean> insertionDeletionListeningOptionPair =
        DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(parameters);
    shouldExtractInsertion = insertionDeletionListeningOptionPair.getLeft();
    shouldExtractDeletion = insertionDeletionListeningOptionPair.getRight();

    pipeName = environment.getPipeName();
    dataRegionId = String.valueOf(environment.getRegionId());
    pipeTaskMeta = environment.getPipeTaskMeta();

    // Metrics related to TsFileEpoch are managed in PipeExtractorMetrics. These metrics are
    // indexed by the taskID of IoTDBDataRegionExtractor. To avoid PipeRealtimeDataRegionExtractor
    // holding a reference to IoTDBDataRegionExtractor, the taskID should be constructed to
    // match that of IoTDBDataRegionExtractor.
    creationTime = environment.getCreationTime();
    pipeID = pipeName + "_" + creationTime;
    taskID = pipeName + "_" + dataRegionId + "_" + creationTime;

    treePattern = TreePattern.parsePipePatternFromSourceParameters(parameters);
    tablePattern = TablePattern.parsePipePatternFromSourceParameters(parameters);

    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(environment.getRegionId()));
    if (dataRegion != null) {
      final String databaseName = dataRegion.getDatabaseName();
      if (databaseName != null) {
        if (PathUtils.isTableModelDatabase(databaseName)) {
          isDbNameCoveredByPattern = tablePattern.coversDb(databaseName);
        } else {
          isDbNameCoveredByPattern = treePattern.coversDb(databaseName);
        }
      }
    }

    startTimePartitionIdLowerBound =
        (realtimeDataExtractionStartTime % TimePartitionUtils.getTimePartitionInterval() == 0)
            ? TimePartitionUtils.getTimePartitionId(realtimeDataExtractionStartTime)
            : TimePartitionUtils.getTimePartitionId(realtimeDataExtractionStartTime) + 1;
    endTimePartitionIdUpperBound =
        (realtimeDataExtractionEndTime % TimePartitionUtils.getTimePartitionInterval() == 0)
            ? TimePartitionUtils.getTimePartitionId(realtimeDataExtractionEndTime)
            : TimePartitionUtils.getTimePartitionId(realtimeDataExtractionEndTime) - 1;

    final boolean isDoubleLiving =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_KEY,
                PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY),
            PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_DEFAULT_VALUE);
    if (isDoubleLiving) {
      isForwardingPipeRequests = false;
    } else {
      isForwardingPipeRequests =
          parameters.getBooleanOrDefault(
              Arrays.asList(
                  PipeSourceConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY,
                  PipeSourceConstant.SOURCE_FORWARDING_PIPE_REQUESTS_KEY),
              PipeSourceConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE);
    }

    if (parameters.hasAnyAttributes(EXTRACTOR_MODS_KEY, SOURCE_MODS_KEY)) {
      shouldTransferModFile =
          parameters.getBooleanOrDefault(
              Arrays.asList(EXTRACTOR_MODS_KEY, SOURCE_MODS_KEY),
              EXTRACTOR_MODS_DEFAULT_VALUE || shouldExtractDeletion);
    } else {
      shouldTransferModFile =
          parameters.getBooleanOrDefault(
              Arrays.asList(SOURCE_MODS_ENABLE_KEY, EXTRACTOR_MODS_ENABLE_KEY),
              EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE || shouldExtractDeletion);
    }

    userName =
        parameters.getStringByKeys(
            PipeSourceConstant.EXTRACTOR_IOTDB_USER_KEY,
            PipeSourceConstant.SOURCE_IOTDB_USER_KEY,
            PipeSourceConstant.EXTRACTOR_IOTDB_USERNAME_KEY,
            PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY);

    skipIfNoPrivileges = getSkipIfNoPrivileges(parameters);

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info(
          "Pipe {}@{}: realtime data region extractor is initialized with parameters: {}.",
          pipeName,
          dataRegionId,
          parameters);
    }
  }

  @Override
  public void start() throws Exception {
    PipeTimePartitionListener.getInstance().startListen(dataRegionId, this);
    PipeInsertionDataNodeListener.getInstance().startListenAndAssign(dataRegionId, this);
  }

  @Override
  public void close() throws Exception {
    if (Objects.nonNull(dataRegionId)) {
      PipeInsertionDataNodeListener.getInstance().stopListenAndAssign(dataRegionId, this);
      PipeTimePartitionListener.getInstance().stopListen(dataRegionId, this);
    }

    synchronized (isClosed) {
      clearPendingQueue();
      isClosed.set(true);
    }
  }

  private void clearPendingQueue() {
    final List<Event> eventsToDrop = new ArrayList<>(pendingQueue.size());

    // processor stage is closed later than extractor stage, {@link supply()} may be called after
    // processor stage is closed. To avoid concurrent issues, we should clear the pending queue
    // before clearing all the reference count of the events in the pending queue.
    pendingQueue.forEach(eventsToDrop::add);
    pendingQueue.clear();

    eventsToDrop.forEach(
        event -> {
          if (event instanceof EnrichedEvent) {
            ((EnrichedEvent) event)
                .clearReferenceCount(PipeRealtimeDataRegionSource.class.getName());
          }
        });
  }

  /**
   * @param event the {@link Event} from the {@link StorageEngine}
   */
  public final void extract(final PipeRealtimeEvent event) {
    // The progress report event shall be directly extracted
    if (event.getEvent() instanceof ProgressReportEvent) {
      extractDirectly(event);
      return;
    }

    if (isDbNameCoveredByPattern) {
      event.skipParsingPattern();
    }

    if (!disableCheckingDataRegionTimePartitionCovering
        && Objects.nonNull(dataRegionTimePartitionIdBound.get())) {
      if (isDataRegionTimePartitionCoveredByTimeRange()) {
        event.skipParsingTime();
      } else {
        // Since we only record the upper and lower bounds that time partition has ever reached, if
        // the time partition cannot be covered by the time range during query, it will not be
        // possible later.
        disableCheckingDataRegionTimePartitionCovering = true;
      }
    }

    // 1. Check if time parsing is necessary. If not, it means that the timestamps of the data
    // contained in this event are definitely within the time range [start time, end time].
    // 2. Check if the event's data timestamps may intersect with the time range. If not, it means
    // that the data timestamps of this event are definitely not within the time range.
    // 3. Check if pattern parsing is necessary. If not, it means that the paths of the data
    // contained in this event are definitely covered by the pattern.
    // 4. Check if the event's data paths may intersect with the pattern. If not, it means that the
    // data of this event is definitely not overlapped with the pattern.
    if ((!event.shouldParseTime() || event.getEvent().mayEventTimeOverlappedWithTimeRange())
        && (!event.shouldParsePattern() || event.getEvent().mayEventPathsOverlappedWithPattern())) {
      if (sloppyTimeRange) {
        // only skip parsing time for events whose data timestamps may intersect with the time range
        event.skipParsingTime();
      }
      if (sloppyPattern) {
        // only skip parsing pattern for events whose data paths may intersect with the pattern
        event.skipParsingPattern();
      }

      doExtract(event);
    } else {
      event.decreaseReferenceCount(PipeRealtimeDataRegionSource.class.getName(), false);
    }

    synchronized (isClosed) {
      if (isClosed.get()) {
        clearPendingQueue();
      }
    }
  }

  protected abstract void doExtract(final PipeRealtimeEvent event);

  protected void extractHeartbeat(final PipeRealtimeEvent event) {
    // Record the pending queue size before trying to put heartbeatEvent into queue
    ((PipeHeartbeatEvent) event.getEvent()).recordExtractorQueueSize(pendingQueue);

    final Event lastEvent = pendingQueue.peekLast();
    if (lastEvent instanceof PipeRealtimeEvent
        && ((PipeRealtimeEvent) lastEvent).getEvent() instanceof PipeHeartbeatEvent
        && (((PipeHeartbeatEvent) ((PipeRealtimeEvent) lastEvent).getEvent()).isShouldPrintMessage()
            || !((PipeHeartbeatEvent) event.getEvent()).isShouldPrintMessage())) {
      // If the last event in the pending queue is a heartbeat event, we should not extract any more
      // heartbeat events to avoid OOM when the pipe is stopped.
      // Besides, the printable event has higher priority to stay in queue to enable metrics report.
      event.decreaseReferenceCount(PipeRealtimeDataRegionSource.class.getName(), false);
      return;
    }

    if (!pendingQueue.waitedOffer(event)) {
      // This would not happen, but just in case.
      // pendingQueue is unbounded, so it should never reach capacity.
      LOGGER.error(
          "extract: pending queue of PipeRealtimeDataRegionHybridExtractor {} "
              + "has reached capacity, discard heartbeat event {}",
          this,
          event);

      // Do not report exception since the PipeHeartbeatEvent doesn't affect
      // the correction of pipe progress.

      // Ignore this event.
      event.decreaseReferenceCount(PipeRealtimeDataRegionSource.class.getName(), false);
    }
  }

  protected void extractDirectly(final PipeRealtimeEvent event) {
    if (!pendingQueue.waitedOffer(event)) {
      // This would not happen, but just in case.
      // Pending is unbounded, so it should never reach capacity.
      final String errorMessage =
          String.format(
              "extract: pending queue of %s %s " + "has reached capacity, discard event %s",
              this.getClass().getSimpleName(), this, event);
      LOGGER.error(errorMessage);
      PipeDataNodeAgent.runtime()
          .report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));

      // Ignore the event.
      event.decreaseReferenceCount(PipeRealtimeDataRegionSource.class.getName(), false);
    }
  }

  protected void maySkipIndex4Event(final PipeRealtimeEvent event) {
    if (event.getEvent() instanceof PipeTsFileInsertionEvent
        || event.getEvent() instanceof PipeInsertNodeTabletInsertionEvent) {
      maySkipProgressIndexForRealtimeEvent(event);
    }
  }

  protected Event supplyHeartbeat(final PipeRealtimeEvent event) {
    if (event.increaseReferenceCount(PipeRealtimeDataRegionSource.class.getName())) {
      return event.getEvent();
    } else {
      // This would not happen, but just in case.
      LOGGER.error(
          "Heartbeat Event {} can not be supplied because "
              + "the reference count can not be increased",
          event.getEvent());

      // Do not report exception since the PipeHeartbeatEvent doesn't affect
      // the correction of pipe progress.

      return null;
    }
  }

  protected Event supplyDirectly(final PipeRealtimeEvent event) {
    if (event.increaseReferenceCount(PipeRealtimeDataRegionSource.class.getName())) {
      return event.getEvent();
    } else {
      // If the event's reference count can not be increased, it means the data represented by
      // this event is not reliable anymore. the data has been lost. we simply discard this
      // event and report the exception to PipeRuntimeAgent.
      final String errorMessage =
          String.format(
              "Event %s can not be supplied because "
                  + "the reference count can not be increased, "
                  + "the data represented by this event is lost",
              event.getEvent());
      LOGGER.error(errorMessage);
      PipeDataNodeAgent.runtime()
          .report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));
      return null;
    }
  }

  public final String getPipeName() {
    return pipeName;
  }

  public final long getCreationTime() {
    return creationTime;
  }

  public final PipeTaskMeta getPipeTaskMeta() {
    return pipeTaskMeta;
  }

  public final boolean shouldExtractInsertion() {
    return shouldExtractInsertion;
  }

  public final boolean shouldExtractDeletion() {
    return shouldExtractDeletion;
  }

  public final TreePattern getTreePattern() {
    return treePattern;
  }

  public final TablePattern getTablePattern() {
    return tablePattern;
  }

  public String getUserName() {
    return userName;
  }

  public boolean isSkipIfNoPrivileges() {
    return skipIfNoPrivileges;
  }

  public final String getDataRegionId() {
    return dataRegionId;
  }

  public final long getRealtimeDataExtractionStartTime() {
    return realtimeDataExtractionStartTime;
  }

  public final long getRealtimeDataExtractionEndTime() {
    return realtimeDataExtractionEndTime;
  }

  public void setDataRegionTimePartitionIdBound(
      @NonNull final Pair<Long, Long> timePartitionIdBound) {
    LOGGER.info(
        "PipeRealtimeDataRegionExtractor({}) observed data region {} time partition growth, recording time partition id bound: {}.",
        taskID,
        dataRegionId,
        timePartitionIdBound);
    dataRegionTimePartitionIdBound.set(timePartitionIdBound);
  }

  private boolean isDataRegionTimePartitionCoveredByTimeRange() {
    final Pair<Long, Long> timePartitionIdBound = dataRegionTimePartitionIdBound.get();
    return startTimePartitionIdLowerBound <= timePartitionIdBound.left
        && timePartitionIdBound.right <= endTimePartitionIdUpperBound;
  }

  public final boolean isForwardingPipeRequests() {
    return isForwardingPipeRequests;
  }

  public abstract boolean isNeedListenToTsFile();

  public abstract boolean isNeedListenToInsertNode();

  public final boolean isShouldTransferModFile() {
    return shouldTransferModFile;
  }

  private void maySkipProgressIndexForRealtimeEvent(final PipeRealtimeEvent event) {
    if (PipeTsFileEpochProgressIndexKeeper.getInstance()
        .isProgressIndexAfterOrEquals(
            dataRegionId,
            pipeName,
            event.getTsFileEpoch().getFilePath(),
            getProgressIndex4RealtimeEvent(event))) {
      event.skipReportOnCommit();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Pipe {} on data region {} skip commit of event {} because it was flushed prematurely.",
            pipeName,
            dataRegionId,
            event.coreReportMessage());
      }
    }
  }

  private ProgressIndex getProgressIndex4RealtimeEvent(final PipeRealtimeEvent event) {
    return event.getEvent() instanceof PipeTsFileInsertionEvent
        ? ((PipeTsFileInsertionEvent) event.getEvent()).forceGetProgressIndex()
        : event.getProgressIndex();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("pipeName", pipeName)
        .add("creationTime", creationTime)
        .add("dataRegionId", dataRegionId)
        .add("pipeTaskMeta", pipeTaskMeta)
        .add("shouldExtractInsertion", shouldExtractInsertion)
        .add("shouldExtractDeletion", shouldExtractDeletion)
        .add("treePattern", treePattern)
        .add("tablePattern", tablePattern)
        .add("isDbNameCoveredByPattern", isDbNameCoveredByPattern)
        .add("realtimeDataExtractionStartTime", realtimeDataExtractionStartTime)
        .add("realtimeDataExtractionEndTime", realtimeDataExtractionEndTime)
        .add(
            "disableCheckingDataRegionTimePartitionCovering",
            disableCheckingDataRegionTimePartitionCovering)
        .add("startTimePartitionIdLowerBound", startTimePartitionIdLowerBound)
        .add("endTimePartitionIdUpperBound", endTimePartitionIdUpperBound)
        .add("dataRegionTimePartitionIdBound", dataRegionTimePartitionIdBound)
        .add("isForwardingPipeRequests", isForwardingPipeRequests)
        .add("shouldTransferModFile", shouldTransferModFile)
        .add("sloppyTimeRange", sloppyTimeRange)
        .add("sloppyPattern", sloppyPattern)
        .add("pendingQueue", pendingQueue)
        .add("isClosed", isClosed)
        .add("taskID", taskID)
        .toString();
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public int getTabletInsertionEventCount() {
    return pendingQueue.getTabletInsertionEventCount();
  }

  public int getTsFileInsertionEventCount() {
    return pendingQueue.getTsFileInsertionEventCount();
  }

  public int getPipeHeartbeatEventCount() {
    return pendingQueue.getPipeHeartbeatEventCount();
  }

  public int getEventCount() {
    return pendingQueue.size();
  }

  public String getTaskID() {
    return taskID;
  }
}
