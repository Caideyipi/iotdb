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

package org.apache.iotdb.db.pipe.extractor.dataregion.realtime.matcher;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.tsfile.common.constant.TsFileConstant.PATH_ROOT;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class CachedSchemaPatternMatcher implements PipeDataRegionMatcher {

  protected static final Logger LOGGER = LoggerFactory.getLogger(CachedSchemaPatternMatcher.class);

  protected static final String TREE_MODEL_EVENT_TABLE_NAME_PREFIX = PATH_ROOT + PATH_SEPARATOR;

  protected final ReentrantReadWriteLock lock;
  private final AccessControl accessControl = Coordinator.getInstance().getAccessControl();
  protected final Set<PipeRealtimeDataRegionExtractor> extractors;

  protected final Cache<IDeviceID, Set<PipeRealtimeDataRegionExtractor>> deviceToExtractorsCache;
  protected final Cache<Pair<String, IDeviceID>, Set<PipeRealtimeDataRegionExtractor>>
      databaseAndTableToExtractorsCache;

  public CachedSchemaPatternMatcher() {
    this.lock = new ReentrantReadWriteLock();
    // Should be thread-safe because the extractors will be returned by {@link #match} and
    // iterated by {@link #assignToExtractor}, at the same time the extractors may be added or
    // removed by {@link #register} and {@link #deregister}.
    this.extractors = new CopyOnWriteArraySet<>();
    this.deviceToExtractorsCache =
        Caffeine.newBuilder()
            .maximumSize(PipeConfig.getInstance().getPipeExtractorMatcherCacheSize())
            .build();
    this.databaseAndTableToExtractorsCache =
        Caffeine.newBuilder()
            .maximumSize(PipeConfig.getInstance().getPipeExtractorMatcherCacheSize())
            .build();
  }

  @Override
  public void register(final PipeRealtimeDataRegionExtractor extractor) {
    lock.writeLock().lock();
    try {
      extractors.add(extractor);
      deviceToExtractorsCache.invalidateAll();
      databaseAndTableToExtractorsCache.invalidateAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void deregister(final PipeRealtimeDataRegionExtractor extractor) {
    lock.writeLock().lock();
    try {
      extractors.remove(extractor);
      deviceToExtractorsCache.invalidateAll();
      databaseAndTableToExtractorsCache.invalidateAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void invalidateCache() {
    lock.writeLock().lock();
    try {
      // TODO: Invalidate device cache
      databaseAndTableToExtractorsCache.invalidateAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int getRegisterCount() {
    lock.readLock().lock();
    try {
      return extractors.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Set<PipeRealtimeDataRegionExtractor> match(final PipeRealtimeEvent event) {
    final Set<PipeRealtimeDataRegionExtractor> matchedExtractors = new HashSet<>();

    lock.readLock().lock();
    try {
      if (extractors.isEmpty()) {
        return matchedExtractors;
      }

      // HeartbeatEvent will be assigned to all extractors
      if (event.getEvent() instanceof PipeHeartbeatEvent) {
        return extractors;
      }

      // TODO: consider table pattern?
      // Deletion event will be assigned to extractors listened to it
      if (event.getEvent() instanceof PipeDeleteDataNodeEvent) {
        return extractors.stream()
            .filter(PipeRealtimeDataRegionExtractor::shouldExtractDeletion)
            .collect(Collectors.toSet());
      }

      for (final Map.Entry<IDeviceID, String[]> entry : event.getSchemaInfo().entrySet()) {
        final IDeviceID deviceID = entry.getKey();

        // TODO: Check the role to determine whether to match with tree model or table model
        if (deviceID instanceof PlainDeviceID
            || deviceID.getTableName().startsWith(TREE_MODEL_EVENT_TABLE_NAME_PREFIX)
            || deviceID.getTableName().equals(PATH_ROOT)) {
          event.markAsTreeModelEvent();
          matchTreeModelEvent(deviceID, entry.getValue(), matchedExtractors);
        } else {
          event.markAsTableModelEvent();
          matchTableModelEvent(
              event.getEvent() instanceof PipeInsertionEvent
                  ? ((PipeInsertionEvent) event.getEvent()).getTableModelDatabaseName()
                  : null,
              deviceID,
              matchedExtractors);
        }

        if (matchedExtractors.size() == extractors.size()) {
          break;
        }
      }
    } finally {
      lock.readLock().unlock();
    }

    return matchedExtractors;
  }

  protected void matchTreeModelEvent(
      final IDeviceID device,
      final String[] measurements,
      final Set<PipeRealtimeDataRegionExtractor> matchedExtractors) {
    // 1. try to get matched extractors from cache, if not success, match them by device
    final Set<PipeRealtimeDataRegionExtractor> extractorsFilteredByDevice =
        deviceToExtractorsCache.get(device, this::filterExtractorsByDevice);
    // this would not happen
    if (extractorsFilteredByDevice == null) {
      LOGGER.warn(
          "Extractors filtered by device is null when matching extractors for tree model event.",
          new Exception());
      return;
    }

    // 2. filter matched candidate extractors by measurements
    if (measurements.length == 0) {
      // `measurements` is empty (only in case of tsfile event). match all extractors.
      //
      // case 1: the pattern can match all measurements of the device.
      // in this case, the extractor can be matched without checking the measurements.
      //
      // case 2: the pattern may match some measurements of the device.
      // in this case, we can't get all measurements efficiently here,
      // so we just ASSUME the extractor matches and do more checks later.
      matchedExtractors.addAll(extractorsFilteredByDevice);
    } else {
      // `measurements` is not empty (only in case of tablet event).
      // Match extractors by measurements.
      extractorsFilteredByDevice.forEach(
          extractor -> {
            if (matchedExtractors.size() == extractors.size()) {
              return;
            }

            final TreePattern pattern = extractor.getTreePattern();
            if (Objects.isNull(pattern) || pattern.isRoot() || pattern.coversDevice(device)) {
              // The pattern can match all measurements of the device.
              matchedExtractors.add(extractor);
            } else {
              for (final String measurement : measurements) {
                // Ignore null measurement for partial insert
                if (measurement == null) {
                  continue;
                }

                if (pattern.matchesMeasurement(device, measurement)) {
                  matchedExtractors.add(extractor);
                  // There would be no more matched extractors because the measurements are
                  // unique
                  break;
                }
              }
            }
          });
    }
  }

  protected Set<PipeRealtimeDataRegionExtractor> filterExtractorsByDevice(final IDeviceID device) {
    final Set<PipeRealtimeDataRegionExtractor> filteredExtractors = new HashSet<>();

    for (final PipeRealtimeDataRegionExtractor extractor : extractors) {
      // Return if the extractor only extract deletion
      if (!extractor.shouldExtractInsertion()) {
        continue;
      }

      final TreePattern treePattern = extractor.getTreePattern();
      if (Objects.isNull(treePattern)
          || (treePattern.isTreeModelDataAllowedToBeCaptured()
              && treePattern.mayOverlapWithDevice(device))) {
        filteredExtractors.add(extractor);
      }
    }

    return filteredExtractors;
  }

  protected void matchTableModelEvent(
      final String databaseName,
      final IDeviceID tableName,
      final Set<PipeRealtimeDataRegionExtractor> matchedExtractors) {
    // this would not happen
    if (databaseName == null) {
      LOGGER.warn(
          "Database name is null when matching extractors for table model event.", new Exception());
      return;
    }

    final Set<PipeRealtimeDataRegionExtractor> extractorsFilteredByDatabaseAndTable =
        databaseAndTableToExtractorsCache.get(
            new Pair<>(databaseName, tableName), this::filterExtractorsByDatabaseAndTable);
    // this would not happen
    if (extractorsFilteredByDatabaseAndTable == null) {
      LOGGER.warn(
          "Extractors filtered by database and table is null when matching extractors for table model event.",
          new Exception());
      return;
    }
    matchedExtractors.addAll(extractorsFilteredByDatabaseAndTable);
  }

  protected Set<PipeRealtimeDataRegionExtractor> filterExtractorsByDatabaseAndTable(
      final Pair<String, IDeviceID> databaseNameAndTableName) {
    final Set<PipeRealtimeDataRegionExtractor> filteredExtractors = new HashSet<>();

    for (final PipeRealtimeDataRegionExtractor extractor : extractors) {
      // Return if the extractor only extract deletion
      if (!extractor.shouldExtractInsertion()) {
        continue;
      }

      final TablePattern tablePattern = extractor.getTablePattern();
      if (matchesTablePattern(tablePattern, databaseNameAndTableName)
          && (!extractor.isSkipIfNoPrivileges()
              || notFilteredByAccess(extractor.getUserName(), databaseNameAndTableName))) {
        filteredExtractors.add(extractor);
      }
    }

    return filteredExtractors;
  }

  private boolean matchesTablePattern(
      final TablePattern tablePattern, final Pair<String, IDeviceID> databaseNameAndTableName) {
    return Objects.isNull(tablePattern)
        || (tablePattern.isTableModelDataAllowedToBeCaptured()
            && tablePattern.matchesDatabase(databaseNameAndTableName.getLeft())
            && tablePattern.matchesTable(databaseNameAndTableName.getRight().getTableName()));
  }

  private boolean notFilteredByAccess(
      final String userName, final Pair<String, IDeviceID> databaseNameAndTableName) {
    try {
      accessControl.checkCanSelectFromTable(
          userName,
          new QualifiedObjectName(
              databaseNameAndTableName.getLeft(),
              databaseNameAndTableName.getRight().getTableName()));
      return true;
    } catch (final Exception e) {
      return false;
    }
  }

  @Override
  public void clear() {
    lock.writeLock().lock();
    try {
      extractors.clear();
      deviceToExtractorsCache.invalidateAll();
      deviceToExtractorsCache.cleanUp();
      databaseAndTableToExtractorsCache.invalidateAll();
      databaseAndTableToExtractorsCache.cleanUp();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
